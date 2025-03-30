import asyncio
import time
import logging
import os
import json
import aiohttp
import requests
import dotenv
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass
import uuid
import concurrent.futures
from util.mongodb_utils import get_mongo_collection

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm_service")

@dataclass
class LLMConfig:
    """Configuração para um modelo LLM"""
    model_name: str
    api_key: str
    api_base: str = None
    max_tokens: int = 4000
    timeout: float = 60.0
    temperature: float = 0.7
    max_retries: int = 3

class LLMResponse:
    """Resposta simplificada de um modelo LLM"""
    def __init__(self, text: str, model_name: str, request_id: str = None, 
                 tokens: Dict = None, latency_ms: float = 0, status: str = "success", 
                 error: str = None):
        self.text = text
        self.request_id = request_id or str(uuid.uuid4())
        self.model_name = model_name
        self.tokens = tokens or {}
        self.latency_ms = latency_ms
        self.status = status
        self.error = error
        self.timestamp = time.time()

# =============== UTILITÁRIOS PARA DETECTAR PROVEDOR ===============

def determine_model_type(config):
    """Determina o tipo de modelo com base na configuração"""
    # Primeiro verificar pela API base, que é mais confiável
    if config.api_base:
        if "openai.com" in config.api_base:
            return "openai"
        elif "anthropic.com" in config.api_base:
            return "anthropic"
        elif "generativelanguage.googleapis.com" in config.api_base:
            return "google"
    
    # Inferir pelo nome do modelo
    if config.model_name.startswith("gpt-"):
        return "openai"
    elif config.model_name.startswith("claude-"):
        return "anthropic"
    elif config.model_name.startswith("gemini-"):
        return "google"
    
    # Fallback para OpenAI como padrão
    return "openai"

# =============== FUNÇÕES DE API ===============

# Versão síncrona - OpenAI
def call_openai_sync(prompt, config):
    api_url = config.api_base or "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.api_key}"
    }
    payload = {
        "model": config.model_name,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": config.max_tokens,
        "temperature": config.temperature
    }
    
    response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=config.timeout
    )
    
    if response.status_code != 200:
        raise Exception(f"OpenAI API error: {response.status_code}, {response.text}")
    
    result = response.json()
    text = result["choices"][0]["message"]["content"]
    tokens = {
        "prompt": result["usage"]["prompt_tokens"],
        "completion": result["usage"]["completion_tokens"],
        "total": result["usage"]["total_tokens"]
    }
    
    return LLMResponse(
        text=text,
        model_name=config.model_name,
        tokens=tokens
    )

# Versão síncrona - Anthropic
def call_anthropic_sync(prompt, config):
    api_url = config.api_base or "https://api.anthropic.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": config.api_key,
        "anthropic-version": "2023-06-01"
    }
    payload = {
        "model": config.model_name,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": config.max_tokens
    }
    
    response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=config.timeout
    )
    
    if response.status_code != 200:
        raise Exception(f"Anthropic API error: {response.status_code}, {response.text}")
    
    result = response.json()
    text = result["content"][0]["text"]
    
    return LLMResponse(
        text=text,
        model_name=config.model_name
    )

# Versão síncrona - Google
def call_google_sync(prompt, config):
    model_name = config.model_name
    
    # Garantir que temos uma URL base válida
    if not config.api_base or not (config.api_base.startswith("http://") or config.api_base.startswith("https://")):
        # Fallback para URL padrão v1beta
        config.api_base = "https://generativelanguage.googleapis.com/v1beta"
        logger.warning(f"URL base inválida, usando fallback: {config.api_base}")
    
    # Determinar versão da API (v1 ou v1beta)
    api_version = "v1"
    if "v1beta" in config.api_base:
        api_version = "v1beta"
    
    # Tratar nome do modelo corretamente
    # Não adicionar prefixo models/ se estamos usando v1beta ou se já está no caminho
    if model_name.startswith("models/"):
        # Se já estiver no formato models/gemini-xxx, remover o prefixo para v1beta
        if api_version == "v1beta":
            model_path = model_name.replace("models/", "")
        else:
            model_path = model_name
    else:
        # Se não tiver o prefixo models/
        if api_version == "v1beta" or "v1beta" in config.api_base:
            # Para v1beta, usar o nome diretamente
            model_path = model_name
        else:
            # Para v1, adicionar o prefixo models/
            model_path = f"models/{model_name}"
    
    # Construir URL base
    base_url = config.api_base.rstrip("/")
    
    # Construir URL completa
    # Formato v1: /v1/models/MODEL:generateContent
    # Formato v1beta: /v1beta/models/MODEL:generateContent (ou apenas MODEL sem o models/)
    if "generateContent" in base_url:
        # URL já inclui o método
        api_url = f"{base_url}?key={config.api_key}"
    elif api_version == "v1beta":
        # Para v1beta, não é necessário o prefixo models/ em alguns casos
        if "/models/" in base_url:
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
        else:
            api_url = f"{base_url}/models/{model_path}:generateContent?key={config.api_key}"
    else:
        # Para v1, formato padrão com models/
        if "/models/" in base_url and not model_path.startswith("models/"):
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
        else:
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
    
    # Log com URL mascarada (sem a chave API)
    masked_url = api_url.replace(config.api_key, "API_KEY_HIDDEN")
    logger.info(f"Chamando Google API: {masked_url}")
    
    # Maximum number of retry attempts for empty responses
    max_empty_retries = 3
    retry_count = 0
    
    while retry_count <= max_empty_retries:
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": config.temperature,
                "maxOutputTokens": config.max_tokens,
                "topP": 0.95,
                "stopSequences": []
            },
            "safetySettings": [
                {
                    "category": "HARM_CATEGORY_HARASSMENT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                },
                {
                    "category": "HARM_CATEGORY_HATE_SPEECH",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                },
                {
                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                },
                {
                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                }
            ]
        }
        
        # Fazer a requisição
        try:
            logger.info(f"Enviando requisição para modelo {model_name} (tentativa {retry_count + 1})")
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=config.timeout
            )
            
            # Verificar status e mostrar detalhes do erro se ocorrer
            if response.status_code != 200:
                error_detail = response.text
                logger.error(f"Google API error: {response.status_code}, {error_detail}")
                raise Exception(f"Google API error: {response.status_code}, {error_detail}")
            
            result = response.json()
            
            # Log para depuração da estrutura da resposta
            logger.info(f"Estrutura da resposta: {list(result.keys())}")
            
            # Extrair texto
            text = ""
            if "candidates" in result and len(result["candidates"]) > 0:
                candidate = result["candidates"][0]
                logger.info(f"Estrutura do candidate: {list(candidate.keys())}")
                
                # Verificar campo finishReason que pode indicar problemas
                if "finishReason" in candidate:
                    logger.info(f"Finish reason: {candidate['finishReason']}")
                    
                    # Se finishReason for diferente de STOP, pode ser um erro
                    if candidate['finishReason'] != "STOP" and candidate['finishReason'] != "STOP_SEQUENCE":
                        logger.warning(f"Finish reason não é STOP: {candidate['finishReason']}")
                        
                        # Se for SAFETY, isso significa que o conteúdo foi bloqueado
                        if candidate['finishReason'] == "SAFETY":
                            logger.warning("Resposta bloqueada por questões de segurança")
                            if retry_count < max_empty_retries:
                                retry_count += 1
                                time.sleep(1)
                                continue
                
                if "content" in candidate:
                    content = candidate["content"]
                    logger.info(f"Estrutura do content: {list(content.keys())}")
                    
                    if "parts" in content:
                        parts = content["parts"]
                        logger.info(f"Número de parts: {len(parts)}")
                        
                        for i, part in enumerate(parts):
                            logger.info(f"Part {i} keys: {list(part.keys())}")
                            if "text" in part:
                                text += part["text"]
                                logger.info(f"Texto encontrado em part {i}: {part['text'][:30]}...")
            
            # Se não houver candidates mas temos usageMetadata, é uma resposta vazia
            elif "usageMetadata" in result and not text and retry_count < max_empty_retries:
                logger.warning(f"Resposta vazia recebida (sem 'candidates'). Tentando novamente ({retry_count + 1}/{max_empty_retries})")
                retry_count += 1
                
                # Pequena pausa antes de tentar novamente
                time.sleep(1)
                continue
                
            # Verificar outras estruturas se não tivermos candidates
            elif "usageMetadata" in result and "promptTokenCount" in result["usageMetadata"] and retry_count >= max_empty_retries:
                # Após várias tentativas, assumimos que o modelo não está gerando texto
                logger.warning(f"Após {max_empty_retries} tentativas, o modelo não gerou texto.")
                text = "O modelo não conseguiu gerar uma resposta. Por favor, tente novamente com um prompt diferente."
            
            # Verificar outras estruturas possíveis (fallbacks)
            elif "text" in result:
                text = result["text"]
                logger.info("Texto encontrado diretamente no resultado")
            elif "choices" in result and len(result["choices"]) > 0:
                choice = result["choices"][0]
                if "message" in choice and "content" in choice["message"]:
                    text = choice["message"]["content"]
                    logger.info("Texto encontrado no formato OpenAI")
            
            # Se ainda não temos texto e não é a última tentativa
            if not text and retry_count < max_empty_retries:
                # Dump da resposta para log
                import json
                logger.warning(f"Não consegui extrair texto da resposta. Estrutura completa: {json.dumps(result, indent=2)}")
                retry_count += 1
                time.sleep(1)
                continue
            
            # Se temos texto ou já tentamos o máximo de vezes
            logger.info(f"Resposta final extraída: {text[:50]}...")
            return LLMResponse(
                text=text,
                model_name=config.model_name
            )
        
        except requests.RequestException as e:
            logger.error(f"Erro na requisição HTTP: {str(e)}")
            raise Exception(f"Erro na requisição para Google API: {str(e)}")
            except Exception as e:
            logger.error(f"Erro ao processar resposta: {str(e)}")
            raise
    
    # Se chegar aqui, todas as tentativas falharam
    return LLMResponse(
        text="Não foi possível obter uma resposta do modelo após várias tentativas.",
        model_name=config.model_name,
                    status="error",
        error="Respostas vazias persistentes"
    )

# Versão assíncrona - OpenAI
async def call_openai_async(prompt, config):
        api_url = config.api_base or "https://api.openai.com/v1/chat/completions"
        
        async with aiohttp.ClientSession() as session:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config.api_key}"
            }
            
            payload = {
                "model": config.model_name,
            "messages": [{"role": "user", "content": prompt}],
                "max_tokens": config.max_tokens,
                "temperature": config.temperature
            }
            
            async with session.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=config.timeout)
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"OpenAI API error: {response.status}, {error_text}")
                
                result = await response.json()
                
                text = result["choices"][0]["message"]["content"]
            tokens = {
                "prompt": result["usage"]["prompt_tokens"],
                "completion": result["usage"]["completion_tokens"],
                "total": result["usage"]["total_tokens"]}
                
            return LLMResponse(
                    text=text,
                    model_name=config.model_name,
            tokens=tokens
        )

# Versão assíncrona - Anthropic
async def call_anthropic_async(prompt, config):
    api_url = config.api_base or "https://api.anthropic.com/v1/messages"
        
    async with aiohttp.ClientSession() as session:
        headers = {
            "Content-Type": "application/json",
            "x-api-key": config.api_key,
            "anthropic-version": "2023-06-01"
        }
        
        payload = {
            "model": config.model_name,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": config.max_tokens
        }
        
    async with session.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=config.timeout)
    ) as response:
        if response.status != 200:
            error_text = await response.text()
            raise Exception(f"Anthropic API error: {response.status}, {error_text}")
        
        
        result = await response.json()
        text = result["content"][0]["text"]
            
        return LLMResponse(
            text=text,
        model_name=config.model_name
                    )

# Versão assíncrona - Google
async def call_google_async(prompt, config):
    model_name = config.model_name
    
    # Garantir que temos uma URL base válida
    if not config.api_base or not (config.api_base.startswith("http://") or config.api_base.startswith("https://")):
        # Fallback para URL padrão v1beta
        config.api_base = "https://generativelanguage.googleapis.com/v1beta"
        logger.warning(f"URL base inválida, usando fallback: {config.api_base}")
    
    # Determinar versão da API (v1 ou v1beta)
    api_version = "v1"
    if "v1beta" in config.api_base:
        api_version = "v1beta"
    
    # Tratar nome do modelo corretamente
    # Não adicionar prefixo models/ se estamos usando v1beta ou se já está no caminho
    if model_name.startswith("models/"):
        # Se já estiver no formato models/gemini-xxx, remover o prefixo para v1beta
        if api_version == "v1beta":
            model_path = model_name.replace("models/", "")
        else:
            model_path = model_name
    else:
        # Se não tiver o prefixo models/
        if api_version == "v1beta" or "v1beta" in config.api_base:
            # Para v1beta, usar o nome diretamente
            model_path = model_name
        else:
            # Para v1, adicionar o prefixo models/
            model_path = f"models/{model_name}"
    
    # Construir URL base
    base_url = config.api_base.rstrip("/")
    
    # Construir URL completa
    # Formato v1: /v1/models/MODEL:generateContent
    # Formato v1beta: /v1beta/models/MODEL:generateContent (ou apenas MODEL sem o models/)
    if "generateContent" in base_url:
        # URL já inclui o método
        api_url = f"{base_url}?key={config.api_key}"
    elif api_version == "v1beta":
        # Para v1beta, não é necessário o prefixo models/ em alguns casos
        if "/models/" in base_url:
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
        else:
            api_url = f"{base_url}/models/{model_path}:generateContent?key={config.api_key}"
    else:
        # Para v1, formato padrão com models/
        if "/models/" in base_url and not model_path.startswith("models/"):
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
        else:
            api_url = f"{base_url}/{model_path}:generateContent?key={config.api_key}"
    
    # Log com URL mascarada (sem a chave API)
    masked_url = api_url.replace(config.api_key, "API_KEY_HIDDEN")
    logger.info(f"Chamando Google API: {masked_url}")
    
    # Maximum number of retry attempts for empty responses
    max_empty_retries = 3
    retry_count = 0
    
    while retry_count <= max_empty_retries:
        async with aiohttp.ClientSession() as session:
            headers = {"Content-Type": "application/json"}
            
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": config.temperature,
                    "maxOutputTokens": config.max_tokens,
                    "topP": 0.95,
                    "stopSequences": []
                },
                "safetySettings": [
                    {
                        "category": "HARM_CATEGORY_HARASSMENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_HATE_SPEECH",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    }
                ]
            }
            
            try:
                logger.info(f"Enviando requisição para modelo {model_name} (tentativa {retry_count + 1})")
                async with session.post(
                    api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=config.timeout)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Google API error: {response.status}, {error_text}")
                        raise Exception(f"Google API error: {response.status}, {error_text}")
                    
                    result = await response.json()
                    
                    # Log para depuração da estrutura da resposta
                    logger.info(f"Estrutura da resposta: {list(result.keys())}")
                    
                    # Extrair texto
                    text = ""
                    if "candidates" in result and len(result["candidates"]) > 0:
                        candidate = result["candidates"][0]
                        logger.info(f"Estrutura do candidate: {list(candidate.keys())}")
                        
                        # Verificar campo finishReason que pode indicar problemas
                        if "finishReason" in candidate:
                            logger.info(f"Finish reason: {candidate['finishReason']}")
                            
                            # Se finishReason for diferente de STOP, pode ser um erro
                            if candidate['finishReason'] != "STOP" and candidate['finishReason'] != "STOP_SEQUENCE":
                                logger.warning(f"Finish reason não é STOP: {candidate['finishReason']}")
                                
                                # Se for SAFETY, isso significa que o conteúdo foi bloqueado
                                if candidate['finishReason'] == "SAFETY":
                                    logger.warning("Resposta bloqueada por questões de segurança")
                                    if retry_count < max_empty_retries:
                                        retry_count += 1
                                        await asyncio.sleep(1)
                                        continue
                        
                        if "content" in candidate:
                            content = candidate["content"]
                            logger.info(f"Estrutura do content: {list(content.keys())}")
                            
                            if "parts" in content:
                                parts = content["parts"]
                                logger.info(f"Número de parts: {len(parts)}")
                                
                                for i, part in enumerate(parts):
                                    logger.info(f"Part {i} keys: {list(part.keys())}")
                                if "text" in part:
                                    text += part["text"]
                                    logger.info(f"Texto encontrado em part {i}: {part['text'][:30]}...")
                    
                    # Se não houver candidates mas temos usageMetadata, é uma resposta vazia
                    elif "usageMetadata" in result and not text and retry_count < max_empty_retries:
                        logger.warning(f"Resposta vazia recebida (sem 'candidates'). Tentando novamente ({retry_count + 1}/{max_empty_retries})")
                        retry_count += 1
                        
                        # Pequena pausa antes de tentar novamente
                        await asyncio.sleep(1)
                        continue
                        
                    # Verificar outras estruturas se não tivermos candidates
                    elif "usageMetadata" in result and "promptTokenCount" in result["usageMetadata"] and retry_count >= max_empty_retries:
                        # Após várias tentativas, assumimos que o modelo não está gerando texto
                        logger.warning(f"Após {max_empty_retries} tentativas, o modelo não gerou texto.")
                        text = "O modelo não conseguiu gerar uma resposta. Por favor, tente novamente com um prompt diferente."
                    
                    # Verificar outras estruturas possíveis (fallbacks)
                    elif "text" in result:
                        text = result["text"]
                        logger.info("Texto encontrado diretamente no resultado")
                    elif "choices" in result and len(result["choices"]) > 0:
                        choice = result["choices"][0]
                        if "message" in choice and "content" in choice["message"]:
                            text = choice["message"]["content"]
                            logger.info("Texto encontrado no formato OpenAI")
                    
                    # Se ainda não temos texto e não é a última tentativa
                    if not text and retry_count < max_empty_retries:
                        # Dump da resposta para log
                        import json
                        logger.warning(f"Não consegui extrair texto da resposta. Estrutura completa: {json.dumps(result, indent=2)}")
                        retry_count += 1
                        await asyncio.sleep(1)
                        continue
                    
                    # Se temos texto ou já tentamos o máximo de vezes
                    logger.info(f"Resposta final extraída: {text[:50]}...")
                    return LLMResponse(
                        text=text,
                        model_name=config.model_name
                    )
            
            except aiohttp.ClientError as e:
                logger.error(f"Erro na requisição HTTP assíncrona: {str(e)}")
                raise Exception(f"Erro na requisição para Google API: {str(e)}")
            except Exception as e:
                logger.error(f"Erro ao processar resposta: {str(e)}")
                raise

    # Se chegar aqui, todas as tentativas falharam
    return LLMResponse(
        text="Não foi possível obter uma resposta do modelo após várias tentativas.",
        model_name=config.model_name,
        status="error",
        error="Respostas vazias persistentes"
    )

# =============== FUNÇÕES DE ACESSO AO MONGODB ===============

def get_model_config_from_mongodb(model_name="gpt-3.5-turbo", fallback_models=None):
    """Obtém configurações de modelos do MongoDB de forma simplificada"""
    if not fallback_models:
        fallback_models = ["gpt-4o", "claude-3-5-sonnet-20240620"]
    
    try:
        # Buscar na coleção "llms"
        llms_collection = get_mongo_collection(db_name="crewai_db", collection_name="llms")
        all_models = [model_name] + fallback_models
        
        # Buscar todos os modelos de uma vez
        records = list(llms_collection.find({"model": {"$in": all_models}}))
        
        # Debug para mostrar o que foi encontrado
        logger.info(f"Modelos buscados: {all_models}")
        logger.info(f"Registros encontrados: {len(records)}")
        if len(records) > 0:
            for rec in records:
                logger.info(f"Encontrado: {rec.get('model')}, provider: {rec.get('provider', 'não especificado')}, URL: {rec.get('baseURL', 'não especificada')}")
        
        if not records:
            logger.warning(f"Nenhum modelo encontrado no MongoDB para: {model_name} ou fallbacks")
            return None, []
        
        # Processar configurações
        configs = []
        for record in records:
            model = record.get("model", "")
            provider = record.get("provider", "").upper() if record.get("provider") else ""
            base_url = record.get("baseURL")
            
            # Log para depuração
            logger.info(f"Processando modelo: {model}, provedor: {provider}, URL base: {base_url}")
            
            # Determinar a base URL com base no provedor e modelo
            api_base = None
            
            # Verificar se URL já foi definida no registro e tem formato de URL
            if base_url and (base_url.startswith("http://") or base_url.startswith("https://")):
                api_base = base_url
                logger.info(f"Usando URL base do registro: {api_base}")
            # Caso contrário, inferir com base no provedor ou nome do modelo
            elif provider == "OPENAI" or (not provider and model.startswith("gpt-")):
                api_base = "https://api.openai.com/v1/chat/completions"
                logger.info(f"Usando URL padrão para OpenAI: {api_base}")
            elif provider == "ANTHROPIC" or (not provider and model.startswith("claude-")):
                api_base = "https://api.anthropic.com/v1/messages"
                logger.info(f"Usando URL padrão para Anthropic: {api_base}")
            elif provider == "GOOGLE" or (not provider and (model.startswith("gemini-") or model.startswith("models/gemini"))):
                # Para modelos mais novos como gemini-2.0-flash ou 2.5, usar v1beta
                if model.startswith("gemini-2") or "2.0" in model or "2.5" in model:
                    api_base = "https://generativelanguage.googleapis.com/v1beta"
                    logger.info(f"Usando URL v1beta para Gemini novo: {api_base}")
                else:
                    # Para outros modelos Gemini, usar v1
                    api_base = "https://generativelanguage.googleapis.com/v1"
                    logger.info(f"Usando URL v1 para Gemini: {api_base}")
            else:
                # Se não conseguimos determinar a URL base, use a padrão v1beta para Gemini
                if model.startswith("gemini-"):
                    api_base = "https://generativelanguage.googleapis.com/v1beta"
                    logger.info(f"Usando URL v1beta padrão para modelo Gemini desconhecido: {api_base}")
                else:
                    # Fallback para OpenAI
                    api_base = "https://api.openai.com/v1/chat/completions"
                    logger.warning(f"Não foi possível determinar URL para {model}, usando padrão OpenAI")
            
            # Verificar se temos uma API key
            api_key = record.get("apiKey")
            if not api_key:
                logger.warning(f"API key não encontrada para {model}, pulando...")
                continue
            
            # Criar a config
            config = LLMConfig(
                model_name=model,
                api_key=api_key,
                api_base=api_base
            )
            configs.append(config)
        
        # Separar configuração principal e fallbacks
        primary_config = next((c for c in configs if c.model_name == model_name), None)
        fallback_configs = [c for c in configs if c.model_name != model_name]
        
        if primary_config:
            logger.info(f"Configuração primária encontrada para {model_name}")
            logger.info(f"API base: {primary_config.api_base}")
        else:
            logger.warning(f"Configuração primária não encontrada para {model_name}")
        
        return primary_config, fallback_configs
    
    except Exception as e:
        logger.error(f"Erro ao buscar configurações do MongoDB: {e}")
        return None, []

# =============== FUNÇÕES PRINCIPAIS ===============

def process_prompt_sync(prompt, config, fallback_configs=None):
    """Processa um único prompt de forma síncrona com fallbacks"""
    fallback_configs = fallback_configs or []
    model_type = determine_model_type(config)
    
    # Primeira tentativa com modelo principal
    try:
        logger.info(f"Enviando prompt para {config.model_name} (tipo: {model_type})")
        
        if model_type == "openai":
            return call_openai_sync(prompt, config)
        elif model_type == "anthropic":
            return call_anthropic_sync(prompt, config)
        elif model_type == "google":
            return call_google_sync(prompt, config)
        else:
            # Usar OpenAI como fallback padrão
            return call_openai_sync(prompt, config)
    
    except Exception as e:
        logger.warning(f"Erro com modelo principal {config.model_name}: {e}")
        
        # Tentar com fallbacks
        for fallback_config in fallback_configs:
            try:
                fallback_type = determine_model_type(fallback_config)
                logger.info(f"Tentando fallback: {fallback_config.model_name} (tipo: {fallback_type})")
                
                if fallback_type == "openai":
                    return call_openai_sync(prompt, fallback_config)
                elif fallback_type == "anthropic":
                    return call_anthropic_sync(prompt, fallback_config)
                elif fallback_type == "google":
                    return call_google_sync(prompt, fallback_config)
                else:
                    return call_openai_sync(prompt, fallback_config)
            
            except Exception as fallback_e:
                logger.warning(f"Erro com fallback {fallback_config.model_name}: {fallback_e}")
                continue
        
        # Se chegou aqui, todos os modelos falharam
        logger.error("Todos os modelos falharam")
        raise Exception(f"Todos os modelos falharam. Erro original: {str(e)}")

async def process_prompt_async(prompt, config, fallback_configs=None):
    """Processa um único prompt de forma assíncrona com fallbacks"""
    fallback_configs = fallback_configs or []
    model_type = determine_model_type(config)
    
    # Primeira tentativa com modelo principal
    try:
        logger.info(f"Enviando prompt para {config.model_name} (tipo: {model_type})")
        
        if model_type == "openai":
            return await call_openai_async(prompt, config)
        elif model_type == "anthropic":
            return await call_anthropic_async(prompt, config)
        elif model_type == "google":
            return await call_google_async(prompt, config)
        else:
            # Usar OpenAI como fallback padrão
            return await call_openai_async(prompt, config)
    
    except Exception as e:
        logger.warning(f"Erro com modelo principal {config.model_name}: {e}")
        
        # Tentar com fallbacks
        for fallback_config in fallback_configs:
            try:
                fallback_type = determine_model_type(fallback_config)
                logger.info(f"Tentando fallback: {fallback_config.model_name} (tipo: {fallback_type})")
                
                if fallback_type == "openai":
                    return await call_openai_async(prompt, fallback_config)
                elif fallback_type == "anthropic":
                    return await call_anthropic_async(prompt, fallback_config)
                elif fallback_type == "google":
                    return await call_google_async(prompt, fallback_config)
                else:
                    return await call_openai_async(prompt, fallback_config)
            
            except Exception as fallback_e:
                logger.warning(f"Erro com fallback {fallback_config.model_name}: {fallback_e}")
                continue
        
        # Se chegou aqui, todos os modelos falharam
        logger.error("Todos os modelos falharam")
        raise Exception(f"Todos os modelos falharam. Erro original: {str(e)}")

# =============== FUNÇÕES PÚBLICAS ===============

def execute_with_threads(prompts, model_name="gpt-3.5-turbo", max_tokens=1000, timeout=30.0, max_workers=3, temperature=0.7):
    """
    Função síncrona para processar prompts usando threads
    
    Args:
        prompts: Lista de prompts ou string com um único prompt
        model_name: Nome do modelo a ser usado
        max_tokens: Limite máximo de tokens na resposta
        timeout: Timeout em segundos
        max_workers: Número máximo de workers (threads)
        temperature: Temperatura para controle de criatividade (0.0 a 1.0)
        
    Returns:
        Texto da resposta ou lista de respostas
    """
    # Garantir que variáveis de ambiente estão carregadas
    dotenv.load_dotenv()
    
    # Normalizar entrada
    single_input = isinstance(prompts, str)
    if single_input:
        prompts = [prompts]
    
    # Buscar configurações
    primary_config, fallback_configs = get_model_config_from_mongodb(model_name)
    
    # Fallback para variáveis de ambiente se necessário
    if not primary_config:
        logger.info(f"Usando API key de variável de ambiente para {model_name}")
        primary_config = LLMConfig(
            model_name=model_name,
            api_key=os.getenv("OPENAI_API_KEY", ""),
            max_tokens=max_tokens,
            timeout=timeout,
            temperature=temperature
        )
    else:
        # Ajustar parâmetros conforme solicitado
        primary_config.max_tokens = max_tokens
        primary_config.timeout = timeout
        primary_config.temperature = temperature
    
    # Função para processar um prompt com thread
    def process_one(prompt):
        try:
            response = process_prompt_sync(prompt, primary_config, fallback_configs)
            return response.text
        except Exception as e:
            logger.error(f"Erro ao processar prompt: {e}")
            return f"ERRO: {str(e)}"
    
    # Processar todos os prompts com threads
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_one, prompts))
    
    # Retornar resultado único ou lista
    return results[0] if single_input else results

async def execute_async(prompts, model_name="gpt-3.5-turbo", max_tokens=1000, timeout=30.0, temperature=0.7):
    """
    Função assíncrona para processar prompts
    
    Args:
        prompts: Lista de prompts ou string com um único prompt
        model_name: Nome do modelo a ser usado
        max_tokens: Limite máximo de tokens na resposta
        timeout: Timeout em segundos
        temperature: Temperatura para controle de criatividade (0.0 a 1.0)
        
    Returns:
        Texto da resposta ou lista de respostas
    """
    # Garantir que variáveis de ambiente estão carregadas
    dotenv.load_dotenv()
    
    # Normalizar entrada
    single_input = isinstance(prompts, str)
    if single_input:
        prompts = [prompts]
    
    # Buscar configurações
    primary_config, fallback_configs = get_model_config_from_mongodb(model_name)
    
    # Fallback para variáveis de ambiente se necessário
    if not primary_config:
        logger.info(f"Usando API key de variável de ambiente para {model_name}")
        primary_config = LLMConfig(
            model_name=model_name,
            api_key=os.getenv("OPENAI_API_KEY", ""),
            max_tokens=max_tokens,
            timeout=timeout,
            temperature=temperature
        )
    else:
        # Ajustar parâmetros conforme solicitado
        primary_config.max_tokens = max_tokens
        primary_config.timeout = timeout
        primary_config.temperature = temperature
    
    # Criar tarefas para todos os prompts
    tasks = [
        process_prompt_async(prompt, primary_config, fallback_configs)
        for prompt in prompts
    ]
    
    # Executar todas as tarefas
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Processar resultados
        texts = []
        for result in results:
            if isinstance(result, Exception):
                texts.append(f"ERRO: {str(result)}")
            else:
                texts.append(result.text)
        
        # Retornar resultado único ou lista
        return texts[0] if single_input else texts
    except Exception as e:
        logger.error(f"Erro ao processar prompts: {e}")
        if single_input:
            return f"ERRO: {str(e)}"
        else:
            return [f"ERRO: {str(e)}"] * len(prompts)

# ========== UTILITÁRIO DE CLUSTER ==========

async def process_cluster(cluster_data, prompt_template, model_name="gpt-3.5-turbo", max_tokens=2000, timeout=120.0, temperature=0.7):
    """
    Processa um cluster de dados usando um modelo LLM.
    
    Args:
        cluster_data: Dados do cluster para processar (serão convertidos para JSON)
        prompt_template: Template de prompt com formatação {cluster_data}
        model_name: Nome do modelo a usar
        max_tokens: Máximo de tokens na resposta
        timeout: Timeout em segundos
        temperature: Temperatura para controle de criatividade (0.0 a 1.0)
        
    Returns:
        String com a resposta ou objeto JSON parseado
    """
    # Preparar o prompt com dados do cluster
        prompt = prompt_template.format(cluster_data=json.dumps(cluster_data, ensure_ascii=False))
        
    # Obter resposta do LLM
    response = await execute_async(prompt, model_name=model_name, max_tokens=max_tokens, timeout=timeout, temperature=temperature)
        
    # Tentar parsear como JSON se parecer JSON
        if response.strip().startswith('{') and response.strip().endswith('}'):
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                return response
    
        return response

# ========== TESTES E EXEMPLOS ==========

def exemplo_sincrono():
    """Exemplo de uso da função síncrona com threads"""
    print("=== Exemplo de uso da função síncrona com threads ===")
    
    # Exemplo com prompt único
    print("\n1. Chamada com prompt único (Gemini 2.0):")
    resposta = execute_with_threads(
        "Explain how AI works in a few words", 
        model_name="gemini-2.0-flash",  # Usando o modelo do exemplo de curl
        max_tokens=100,
        timeout=30.0,
        temperature=0.7  # Adicionando temperatura
    )
    print("\nResposta recebida:")
    print(resposta)
    
    # Exemplo com múltiplos prompts
    print("\n2. Chamada com múltiplos prompts:")
    prompts = [
        "O que é machine learning?",
        "O que é deep learning?",
        "O que é processamento de linguagem natural?"
    ]
    
    respostas = execute_with_threads(
        prompts,
        model_name="gpt-3.5-turbo",  # Usando OpenAI para este exemplo
        max_tokens=100,
        max_workers=3,
        temperature=0.8  # Temperatura mais alta para mais criatividade
    )
    
    print("\nRespostas recebidas:")
    for i, resposta in enumerate(respostas):
        print(f"\nPergunta {i+1}: {prompts[i]}")
        print(f"Resposta: {resposta[:100]}...")
    
    return "Exemplo concluído com sucesso"

async def exemplo_assincrono():
    """Exemplo de uso da função assíncrona"""
    print("=== Exemplo de uso da função assíncrona ===")
    
    # Exemplo com prompt único
    print("\n1. Chamada assíncrona com prompt único:")
    resposta = await execute_async(
        "O que é inteligência artificial?", 
        model_name="gpt-3.5-turbo", 
        max_tokens=100,
        timeout=30.0,
        temperature=0.7  # Adicionando temperatura
    )
    print("\nResposta recebida:")
    print(resposta)
    
    # Exemplo com múltiplos prompts
    print("\n2. Chamada assíncrona com múltiplos prompts:")
    prompts = [
        "O que é machine learning?",
        "O que é deep learning?",
        "O que é processamento de linguagem natural?"
    ]
    
    respostas = await execute_async(
        prompts,
        model_name="gemini-2.0-flash",
        max_tokens=30000,
        temperature=0.8  # Temperatura mais alta para mais criatividade
    )
    
    print("\nRespostas recebidas:")
    for i, resposta in enumerate(respostas):
        print(f"\nPergunta {i+1}: {prompts[i]}")
        print(f"Resposta: {resposta[:100]}...")
    
    return "Exemplo assíncrono concluído com sucesso"

# Se executado diretamente
if __name__ == "__main__":
    # Carregar variáveis de ambiente
    dotenv.load_dotenv()
    
    # Menu de opções
    print("Escolha uma opção:")
    print("1. Executar exemplo síncrono com threads")
    print("2. Executar exemplo assíncrono")
    
    opcao = input("Opção: ")
    
    if opcao == "1":
        # Executar exemplo síncrono
        exemplo_sincrono()
    else:
        # Executar exemplo assíncrono
        try:
            asyncio.run(exemplo_assincrono())
        except KeyboardInterrupt:
            print("\nExecução interrompida pelo usuário")
        except Exception as e:
            print(f"\nErro: {e}")