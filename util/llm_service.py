import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass
import json
import aiohttp
import backoff
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
import os
import dotenv
from util.mongodb_utils import get_mongo_collection
from pymongo import MongoClient

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm_service")

@dataclass
class LLMConfig:
    """Configuração para um modelo LLM específico"""
    model_name: str
    api_key: str
    api_base: str = None
    max_tokens: int = 4000
    timeout: float = 60.0
    temperature: float = 0.7
    max_retries: int = 3
    retry_delay: float = 2.0
    context_window: int = 8000  # Limite de tokens de entrada para o modelo

# Função para buscar configurações de modelos do MongoDB
def get_llm_configs_from_mongodb(model_names=None, use_all=False):
    """
    Busca configurações de modelos LLM da coleção 'llms' no banco de dados crewai_db
    
    Args:
        model_names: Lista de nomes de modelos para buscar. Se None, busca o primeiro disponível.
        use_all: Se True, retorna todos os modelos disponíveis na coleção
        
    Returns:
        Lista de objetos LLMConfig com as configurações dos modelos
    """
    llms_collection = get_mongo_collection(db_name="crewai_db", collection_name="llms")
    
    # Determinar filtro de consulta
    if use_all:
        filter_query = {}
    elif model_names:
        filter_query = {"model": {"$in": model_names}}
    else:
        filter_query = {}  # Buscar qualquer modelo
    
    # Buscar modelos no MongoDB
    llm_records = list(llms_collection.find(filter_query))
    
    if not llm_records:
        logger.warning("Nenhuma configuração de LLM encontrada no MongoDB")
        return []
    
    # Converter registros do MongoDB para objetos LLMConfig
    configs = []
    for record in llm_records:
        # Determinar o tipo de provedor e URL correta
        provider = record.get("provider", "").upper()
        if not provider:
            # Tentar determinar o provedor pelo nome do modelo
            model_name = record.get("model", "")
            if model_name.startswith("gpt-"):
                provider = "OPENAI"
            elif model_name.startswith("claude-"):
                provider = "ANTHROPIC"
            elif model_name.startswith("gemini-"):
                provider = "GOOGLE"
        
        # Determinar a base URL específica para cada provedor
        api_base = None
        if provider == "OPENAI" or record.get("baseURL", "").upper() == "OPENAI":
            api_base = "https://api.openai.com/v1/chat/completions"
        elif provider == "ANTHROPIC" or record.get("baseURL", "").upper() == "ANTHROPIC":
            api_base = "https://api.anthropic.com/v1/messages"
        elif provider == "GOOGLE" or record.get("baseURL", "").upper() == "GOOGLE":
            api_base = "https://generativelanguage.googleapis.com/v1/models"
        else:
            # URL personalizada ou desconhecida
            api_base = record.get("baseURL")
        
        config = LLMConfig(
            model_name=record.get("model"),
            api_key=record.get("apiKey"),
            api_base=api_base
        )
        configs.append(config)
    
    logger.info(f"Carregadas {len(configs)} configurações de LLM do MongoDB")
    return configs

# Função para obter modelo principal e fallbacks da coleção do MongoDB
def get_model_configs(primary_model="gpt-3.5-turbo", fallback_models=None):
    """
    Busca configurações de modelo principal e fallbacks da coleção MongoDB
    
    Args:
        primary_model: Nome do modelo principal a ser usado
        fallback_models: Lista de nomes de modelos para fallback
        
    Returns:
        Tupla com (config_principal, [configs_fallback])
    """
    # Definir modelos fallback padrão se não especificados
    if fallback_models is None:
        fallback_models = ["gpt-4o", "claude-3-5-sonnet-20240620"]
    
    # Buscar todos os modelos necessários de uma vez
    all_models = [primary_model] + fallback_models
    configs = get_llm_configs_from_mongodb(model_names=all_models)
    
    if not configs:
        logger.error("Nenhuma configuração de modelo encontrada no MongoDB")
        return None, []
    
    # Separar modelo principal e fallbacks
    primary_config = None
    fallback_configs = []
    
    for config in configs:
        if config.model_name == primary_model:
            primary_config = config
        elif config.model_name in fallback_models:
            fallback_configs.append(config)
    
    # Se não encontrou o modelo principal, use o primeiro disponível
    if primary_config is None and configs:
        primary_config = configs[0]
        logger.warning(f"Modelo principal {primary_model} não encontrado. Usando {primary_config.model_name}")
    
    return primary_config, fallback_configs

@dataclass
class LLMRequest:
    """Solicitação para um modelo LLM"""
    prompt: str
    model_config: LLMConfig
    priority: int = 0  # Prioridade mais alta = processada primeiro
    callback: Callable = None  # Função opcional para chamar com o resultado
    metadata: Dict[str, Any] = None  # Metadados adicionais
    request_id: str = None  # ID opcional para rastreamento
    
    def __post_init__(self):
        if self.request_id is None:
            self.request_id = str(uuid.uuid4())

class LLMResponse:
    """Resposta de um modelo LLM"""
    def __init__(
        self, 
        text: str, 
        request_id: str,
        model_name: str,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        latency_ms: float = 0,
        status: str = "success",
        error: str = None,
        metadata: Dict[str, Any] = None
    ):
        self.text = text
        self.request_id = request_id
        self.model_name = model_name
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = prompt_tokens + completion_tokens
        self.latency_ms = latency_ms
        self.status = status
        self.error = error
        self.metadata = metadata or {}
        self.timestamp = time.time()

class LLMService:
    """Serviço para gerenciar solicitações LLM de forma robusta e paralela"""
    
    def __init__(self, 
                 max_concurrency: int = 10,
                 default_config: LLMConfig = None,
                 fallback_configs: List[LLMConfig] = None):
        self.max_concurrency = max_concurrency
        self.default_config = default_config
        self.fallback_configs = fallback_configs or []
        
        # Semáforo para controlar concorrência
        self._semaphore = asyncio.Semaphore(max_concurrency)
        
        # Fila de solicitações pendentes (prioridade)
        self._request_queue = asyncio.PriorityQueue()
        
        # Cache de resultados (opcional, baseado em hash de prompt+config)
        self._cache = {}
        
        # Controle de taxa de solicitações por modelo
        self._rate_limits = {}
        
        # Estado do serviço
        self._running = False
        self._worker_task = None
    
    async def start(self):
        """Inicia o serviço de processamento de LLM"""
        if self._running:
            return
        
        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())
        logger.info("LLM Service started")
    
    async def stop(self):
        """Para o serviço de processamento de LLM"""
        if not self._running:
            return
        
        self._running = False
        if self._worker_task:
            await self._worker_task
            self._worker_task = None
        logger.info("LLM Service stopped")
    
    async def submit(self, request: Union[LLMRequest, str]) -> str:
        """Submete uma solicitação para o serviço e aguarda o resultado"""
        if isinstance(request, str):
            request = LLMRequest(prompt=request, model_config=self.default_config)
        
        # Colocar na fila com prioridade (menor número = maior prioridade)
        await self._request_queue.put((request.priority, request))
        
        # Aguardar o resultado via future
        future = asyncio.Future()
        request.callback = lambda resp: future.set_result(resp)
        
        # Aguardar e retornar apenas o texto da resposta para simplicidade
        response = await future
        return response.text
    
    async def submit_batch(self, requests: List[LLMRequest]) -> List[LLMResponse]:
        """Submete um lote de solicitações para processamento em paralelo"""
        futures = []
        
        for request in requests:
            future = asyncio.Future()
            request.callback = lambda resp, fut=future: fut.set_result(resp)
            await self._request_queue.put((request.priority, request))
            futures.append(future)
        
        # Aguardar todos os resultados
        return await asyncio.gather(*futures)
    
    async def _process_queue(self):
        """Worker que processa a fila de solicitações"""
        while self._running:
            try:
                # Obter próxima solicitação da fila
                _, request = await self._request_queue.get()
                
                # Processar com limite de concorrência
                asyncio.create_task(self._process_request(request))
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing request queue: {e}")
    
    async def _process_request(self, request: LLMRequest):
        """Processa uma única solicitação com retry e fallback"""
        async with self._semaphore:
            start_time = time.time()
            response = None
            
            # Tentar com o modelo principal
            try:
                response = await self._call_llm_with_retry(request)
            except Exception as e:
                logger.warning(f"Primary model failed: {e}. Trying fallbacks...")
                
                # Tentar com modelos de fallback
                for fallback_config in self.fallback_configs:
                    try:
                        fallback_request = LLMRequest(
                            prompt=request.prompt,
                            model_config=fallback_config,
                            priority=request.priority,
                            metadata=request.metadata,
                            request_id=request.request_id
                        )
                        response = await self._call_llm_with_retry(fallback_request)
                        break
                    except Exception as fallback_e:
                        logger.warning(f"Fallback model {fallback_config.model_name} failed: {fallback_e}")
            
            # Se todos os modelos falharem
            if response is None:
                response = LLMResponse(
                    text="",
                    request_id=request.request_id,
                    model_name=request.model_config.model_name,
                    status="error",
                    error="All models failed to process the request",
                    latency_ms=(time.time() - start_time) * 1000
                )
            
            # Chamar callback se definido
            if request.callback:
                request.callback(response)
    
    @backoff.on_exception(
        backoff.expo, 
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3
    )
    async def _call_llm_with_retry(self, request: LLMRequest) -> LLMResponse:
        """Chama o LLM com retry automático para erros transitórios"""
        config = request.model_config
        start_time = time.time()
        
        # Verificar se o prompt é muito grande para o modelo
        # Implementação simplificada - você pode usar um tokenizer adequado para o modelo
        estimated_tokens = len(request.prompt.split()) * 1.3  # Estimativa aproximada
        if estimated_tokens > config.context_window:
            raise ValueError(f"Prompt too large for model context window ({estimated_tokens} > {config.context_window})")
        
        # Determinar qual adaptador de modelo usar com base no nome/URL da API
        model_type = self._determine_model_type(config)
        logger.info(f"Usando modelo tipo: {model_type}, nome: {config.model_name}")
        
        try:
            if model_type == "openai":
                response = await self._call_openai(request)
            elif model_type == "anthropic":
                response = await self._call_anthropic(request)
            elif model_type == "google":
                response = await self._call_google(request)
            elif model_type == "local":
                response = await self._call_local_model(request)
            else:
                # Fallback para OpenAI
                logger.warning(f"Tipo de modelo desconhecido: {model_type}, tentando como OpenAI")
                response = await self._call_openai(request)
        except Exception as e:
            logger.error(f"Erro ao chamar LLM ({model_type}): {type(e).__name__}: {e}")
            raise
        
        latency = (time.time() - start_time) * 1000
        logger.info(f"Chamada ao modelo {config.model_name} completada em {latency:.2f}ms")
        return response
    
    def _determine_model_type(self, config: LLMConfig) -> str:
        """Determina o tipo de modelo com base na configuração"""
        # Primeiro verificar pela API base, que é mais confiável
        if config.api_base:
            if "openai.com" in config.api_base:
                return "openai"
            elif "anthropic.com" in config.api_base:
                return "anthropic"
            elif "generativelanguage.googleapis.com" in config.api_base:
                return "google"
            elif any(local in config.api_base for local in ["localhost", "127.0.0.1"]):
                return "local"
        
        # Se não tiver API base, inferir pelo nome do modelo
        if config.model_name.startswith("gpt-"):
            return "openai"
        elif config.model_name.startswith("claude-"):
            return "anthropic"
        elif config.model_name.startswith("gemini-"):
            return "google"
        
        # Fallback para OpenAI como padrão com aviso
        logger.warning(f"Tipo de modelo não reconhecido: {config.model_name}. Usando OpenAI como padrão.")
        return "openai"
    
    async def _call_openai(self, request: LLMRequest) -> LLMResponse:
        """Chama a API da OpenAI"""
        config = request.model_config
        start_time = time.time()
        
        # Garantir que temos a URL correta da API
        api_url = config.api_base or "https://api.openai.com/v1/chat/completions"
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config.api_key}"
            }
            
            payload = {
                "model": config.model_name,
                "messages": [{"role": "user", "content": request.prompt}],
                "max_tokens": config.max_tokens,
                "temperature": config.temperature
            }
            
            try:
                logger.info(f"Chamando API OpenAI para modelo {config.model_name}")
                async with session.post(
                    api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=config.timeout)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Erro OpenAI API: Status {response.status}, {error_text}")
                        raise Exception(f"OpenAI API error: {response.status}, {error_text}")
                    
                    result = await response.json()
                    
                    text = result["choices"][0]["message"]["content"]
                    prompt_tokens = result["usage"]["prompt_tokens"]
                    completion_tokens = result["usage"]["completion_tokens"]
                    
                    return LLMResponse(
                        text=text,
                        request_id=request.request_id,
                        model_name=config.model_name,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        latency_ms=(time.time() - start_time) * 1000,
                    )
            except Exception as e:
                logger.error(f"Erro ao chamar OpenAI: {type(e).__name__}: {e}")
                raise
    
    async def _call_anthropic(self, request: LLMRequest) -> LLMResponse:
        """Chama a API da Anthropic Claude"""
        config = request.model_config
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Content-Type": "application/json",
                "x-api-key": config.api_key,
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": config.model_name,
                "messages": [{"role": "user", "content": request.prompt}],
                "max_tokens": config.max_tokens,
                "temperature": config.temperature
            }
            
            api_url = config.api_base or "https://api.anthropic.com/v1/messages"
            
            async with session.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=config.timeout)
            ) as response:
                result = await response.json()
                
                if response.status != 200:
                    raise Exception(f"Anthropic API error: {result}")
                
                text = result["content"][0]["text"]
                # Nota: Anthropic não fornece contagens de tokens no formato que a OpenAI oferece
                # Estimamos baseado no comprimento do texto
                estimated_tokens = len(text.split()) * 1.3
                
                return LLMResponse(
                    text=text,
                    request_id=request.request_id,
                    model_name=config.model_name,
                    prompt_tokens=int(len(request.prompt.split()) * 1.3),
                    completion_tokens=int(estimated_tokens),
                    latency_ms=(time.time() - start_time) * 1000,
                )
    
    async def _call_local_model(self, request: LLMRequest) -> LLMResponse:
        """Chama um modelo local ou servidor personalizado"""
        config = request.model_config
        start_time = time.time()
        
        # Esta implementação supõe um servidor compatível com a API da OpenAI rodando localmente
        async with aiohttp.ClientSession() as session:
            headers = {"Content-Type": "application/json"}
            
            if config.api_key:
                headers["Authorization"] = f"Bearer {config.api_key}"
            
            payload = {
                "model": config.model_name,
                "prompt": request.prompt,
                "max_tokens": config.max_tokens,
                "temperature": config.temperature
            }
            
            # Usar API padrão se não fornecida
            api_url = config.api_base or "http://localhost:8000/v1/completions"
            
            async with session.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=config.timeout)
            ) as response:
                result = await response.json()
                
                if response.status != 200:
                    raise Exception(f"Local API error: {result}")
                
                # Adapte isso com base na resposta do seu servidor local
                text = result.get("choices", [{}])[0].get("text", "")
                
                return LLMResponse(
                    text=text,
                    request_id=request.request_id,
                    model_name=config.model_name,
                    latency_ms=(time.time() - start_time) * 1000,
                )

    async def _call_google(self, request: LLMRequest) -> LLMResponse:
        """Chama a API do Google (Gemini)"""
        config = request.model_config
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            # URL para o modelo específico com método generateContent
            model_name = config.model_name
            api_url = f"{config.api_base}/{model_name}:generateContent?key={config.api_key}"
            
            headers = {"Content-Type": "application/json"}
            
            payload = {
                "contents": [{"parts": [{"text": request.prompt}]}],
                "generationConfig": {
                    "temperature": config.temperature,
                    "maxOutputTokens": config.max_tokens,
                    "topP": 0.95,
                    "topK": 64
                }
            }
            
            try:
                logger.info(f"Chamando API Google para modelo {config.model_name}")
                async with session.post(
                    api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=config.timeout)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Erro Google API: Status {response.status}, {error_text}")
                        raise Exception(f"Google API error: {response.status}, {error_text}")
                    
                    result = await response.json()
                    
                    # Extrair a resposta do formato da API do Google
                    text = ""
                    if "candidates" in result and len(result["candidates"]) > 0:
                        candidate = result["candidates"][0]
                        if "content" in candidate and "parts" in candidate["content"]:
                            for part in candidate["content"]["parts"]:
                                if "text" in part:
                                    text += part["text"]
                
                    # Google não retorna contagem de tokens
                    return LLMResponse(
                        text=text,
                        request_id=request.request_id,
                        model_name=config.model_name,
                        latency_ms=(time.time() - start_time) * 1000,
                    )
            except Exception as e:
                logger.error(f"Erro ao chamar Google API: {type(e).__name__}: {e}")
                raise

# Exemplo de uso
async def example():
    # Obter configurações do MongoDB em vez de usar variáveis de ambiente diretamente
    primary_config, fallback_configs = get_model_configs(
        primary_model="gpt-3.5-turbo",
        fallback_models=["gpt-4o"]
    )
    
    # Usar fallback para configuração local somente se não encontrar no MongoDB
    if not primary_config:
        primary_config = LLMConfig(
            model_name="gpt-3.5-turbo",
            api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=1000
        )
        logger.warning("Configuração principal não encontrada no MongoDB, usando variável de ambiente")
    
    # Garantir que temos ao menos uma configuração de fallback
    if not fallback_configs:
        # Criar um fallback usando variável de ambiente apenas se necessário
        fallback_config = LLMConfig(
            model_name="gpt-4o",
            api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=1000
        )
        fallback_configs = [fallback_config]
        logger.warning("Nenhuma configuração de fallback encontrada no MongoDB, usando variável de ambiente")
    
    # Iniciar serviço com fallback
    service = LLMService(
        max_concurrency=5,
        default_config=primary_config,
        fallback_configs=fallback_configs
    )
    
    await service.start()
    
    try:
        # Exemplo de solicitação simples
        single_response = await service.submit("Explique brevemente o que é inteligência artificial")
        
        # Exemplo de batch com diferentes prioridades
        batch_requests = [
            LLMRequest(prompt="O que é machine learning?", 
                    model_config=primary_config, 
                    priority=1),  # Prioridade alta
            LLMRequest(prompt="O que é deep learning?", 
                    model_config=primary_config, 
                    priority=2)   # Prioridade média
        ]
        
        batch_responses = await service.submit_batch(batch_requests)
        
        # Retornar os resultados em vez de imprimi-los
        return {
            "single_response": single_response,
            "batch_responses": [resp.text for resp in batch_responses]
        }
    finally:
        await service.stop()

# Função para processar uma lista de prompts
async def process_prompts(prompts, model_name="gpt-3.5-turbo", max_tokens=1000, timeout=60.0):
    """
    Processa uma lista de prompts e retorna as respostas.
    Otimiza fazendo uma requisição simples quando há apenas um prompt.
    
    Args:
        prompts: Lista de strings com prompts para processar
        model_name: Nome do modelo a ser usado
        max_tokens: Limite máximo de tokens para respostas
        timeout: Timeout em segundos
        
    Returns:
        Lista de strings com respostas para cada prompt
    """
    if not prompts:
        return []
    
    # Buscar configuração do modelo do MongoDB    
    primary_config, fallback_configs = get_model_configs(primary_model=model_name)
    
    if not primary_config:
        # Fallback para configuração local se não encontrar no MongoDB
        primary_config = LLMConfig(
            model_name=model_name,
            api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=max_tokens,
            timeout=timeout
        )
    else:
        # Substituir valores de max_tokens e timeout se fornecidos
        primary_config.max_tokens = max_tokens
        primary_config.timeout = timeout
    
    # Iniciar serviço
    service = LLMService(
        max_concurrency=3,
        default_config=primary_config,
        fallback_configs=fallback_configs
    )
    
    await service.start()
    
    try:
        # Caso especial: apenas um prompt
        if len(prompts) == 1:
            response = await service.submit(prompts[0])
            return [response]
        
        # Caso com múltiplos prompts
        requests = [
            LLMRequest(prompt=prompt, model_config=primary_config)
            for prompt in prompts
        ]
        
        responses = await service.submit_batch(requests)
        return [resp.text for resp in responses]
    finally:
        await service.stop()

# Função para processar um cluster usando o LLMService
async def process_cluster(cluster_data, prompt_template, model_name="gpt-3.5-turbo", max_tokens=2000, timeout=120.0):
    """
    Processa um cluster de posts usando o LLMService
    
    Args:
        cluster_data: Dados do cluster a ser processado
        prompt_template: Template de prompt para analisar o cluster
        model_name: Nome do modelo a ser usado (busca do MongoDB)
        max_tokens: Limite máximo de tokens na resposta
        timeout: Timeout em segundos para a chamada da API
        
    Returns:
        String com a análise do cluster ou objeto JSON parseado
    """

    # Buscar configurações do modelo do MongoDB
    primary_config, fallback_configs = get_model_configs(
        primary_model=model_name, 
        fallback_models=["gpt-4o", "claude-3-5-sonnet-20240620"]
    )
    
    if not primary_config:
        # Fallback para configuração local se não encontrar no MongoDB
        primary_config = LLMConfig(
            model_name=model_name,
            api_key=os.getenv("OPENAI_API_KEY"),
            max_tokens=max_tokens,
            timeout=timeout
        )
    else:
        # Substituir valores de max_tokens e timeout se fornecidos
        primary_config.max_tokens = max_tokens
        primary_config.timeout = timeout
    
    # Iniciar serviço com menos concorrência para evitar rate limits
    service = LLMService(
        max_concurrency=2,
        default_config=primary_config,
        fallback_configs=fallback_configs
    )
    
    await service.start()
    
    try:
        # Preparar o prompt com os dados do cluster
        prompt = prompt_template.format(cluster_data=json.dumps(cluster_data, ensure_ascii=False))
        
        # Processar com LLM
        response = await service.submit(prompt)
        
        # Tentar parsear como JSON se a resposta parecer ser JSON
        if response.strip().startswith('{') and response.strip().endswith('}'):
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                return response
        return response
    finally:
        await service.stop()

# Para executar o exemplo em um script independente
if __name__ == "__main__":
    # Exemplo de uso da função process_prompts
    teste = asyncio.run(process_prompts(["Explique brevemente o que é inteligência artificial"], model_name="gpt-4o", max_tokens=1000, timeout=60.0))
    print(teste)
    
    # Executar o exemplo principal
    result = asyncio.run(example())
    print("Resposta única:", result["single_response"])
    for i, resp in enumerate(result["batch_responses"]):
        print(f"Resposta batch {i+1}:", resp)