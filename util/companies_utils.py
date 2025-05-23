from util.mongodb_utils import get_mongo_collection
from util.embedding_utils import get_embedding
from models.companies import Companies
from typing import List, Dict, Any, Optional
from datetime import datetime
from pymongo import errors
import logging
import json
import asyncio
from util.langchain_utils import connect_to_graph_execution


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from util.parsing_utils import extract_brace_arguments, extract_json_from_content
import re
from util.llm_services import execute_llm_with_threads
from concurrent.futures import ThreadPoolExecutor

# normalization helpers
from util.normalization_utils import normalize_name, normalize_ticker

from pymongo import ReturnDocument

def parse_companies(content: str) -> List[Dict[str, Any]]:
    cleaned = extract_json_from_content(content).strip()
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"Company parsing error: {e}")
        return []

def get_candidate_companies(company: str, companies_collection, similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
    """
    Return a list of candidate companies from companies_collection with similarity > threshold.
    Each candidate is a dict with fields: name, ticker, public, parent_company, description, sector.
    """
    embedding = get_embedding(company)
    results = companies_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",  # assuming same index
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": 3,
                "limit": 3,
            }
        },
        {
            "$project": {
                "similarityScore": {"$meta": "vectorSearchScore"},
                "document": "$$ROOT",
            }
        },
    ])
    candidate_list = []
    for result in results:
        score = result.get("similarityScore", 0)
        if score > similarity_threshold:
            doc = result["document"]
            candidate = {
                "id": doc.get("_id", ""),
                "name": doc.get("name", ""),
                "ticker": doc.get("ticker", ""),
                "public": doc.get("public", False),
                "parent_company": doc.get("parent_company", ""),
                "description": doc.get("description", ""),
                "sector": doc.get("sector", "")  # Added sector here
            }
            candidate_list.append(candidate)
    return candidate_list


def grab_tickers_company(target: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Call the ticker-guesser graph with input containing:
      - ticker: target (string)
      - candidates: list of candidate companies (each with fields: name, ticker, public, parent_company, description)
    Returns a dict with fields: name, ticker, public, parent_company, description, already_exists.
    """
    #target="Adecoagro"
    #candidates=[]
    input_payload = json.dumps({
        "ticker": target,
        "candidates": candidates
    })
    response = asyncio.run(
        connect_to_graph_execution("66ec6713966174697a68ed23", initial_message=input_payload)
    )
    content = response[0]["step"]["Ticker Guesser"][-1]["content"]
    # Use parse_companies to parse the JSON output (which should be a list)
    result_list = parse_companies(content)
    return result_list[0] if result_list else {}




def find_similar_company(embedding: List[float], companies_collection, similarity_threshold: float = 0.9) -> Optional[Companies]:
    results = companies_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",  # adjust index name if necessary
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": 10,
                "limit": 10,
            }
        },
        {
            "$project": {
                "similarityScore": {"$meta": "vectorSearchScore"},
                "document": "$$ROOT",
            }
        },
    ])
    results_list = list(results)
    if results_list:
        most_similar = results_list[0]
        if most_similar["similarityScore"] >= similarity_threshold:
            return Companies(**most_similar["document"])
    return None


def intruments_to_companies_ids(instruments: List[str], companies_collection) -> List[str]:
    """
    Convert a list of instrument names to a list of company IDs.
    """
    #instruments = ["Bradesco Banco Brasileiro S.A.", "XP Inc."]
    #companies_collection = get_mongo_collection(collection_name="companies")
    companies_ids = []
        
    if instruments != []:
        
        for company in instruments:
            #company =instruments[1]
            
            try:
                company_embedding = get_embedding(company)
            except Exception as e:
                logger.error(f"Error generating embedding for company '{company}': {e}")
                continue
            existing_company = find_similar_company(company_embedding, companies_collection)
            if existing_company:
                companies_ids.append(existing_company.id)
            else:
                # Use ticker-guesser graph to determine company info
                candidates = get_candidate_companies(company, companies_collection)
                
                ticker_data = grab_tickers_company(company, candidates)
                
                new_company = Companies(
                    name=ticker_data.get("name", company),
                    ticker=ticker_data.get("ticker", ""),
                    public=ticker_data.get("public", False),
                    parent_company=ticker_data.get("parent_company", ""),
                    description=ticker_data.get("description", ""),
                    sector=ticker_data.get("sector", ""),  # Include sector value from ticker_data
                    embedding=company_embedding,
                    created_at=datetime.now(),
                )
                
                try:
                    if ticker_data.get("already_exists", False):
                        new_company.id = ticker_data.get("id", "")
                        result = companies_collection.update_one(
                            {"id": ticker_data.get("id", "")},
                            {"$set": new_company.model_dump(by_alias=True)}
                        )
                        logger.info(f"Updated existing company '{company}' with ID: {new_company.id}")
                    else:
                        result = companies_collection.insert_one(new_company.model_dump(by_alias=True))
                        new_company.id = result.inserted_id if result.inserted_id else new_company.id
                        logger.info(f"Inserted new company '{company}' with ID: {new_company.id}")
                    
                    companies_ids.append(new_company.id)
                    
                except errors.PyMongoError as e:
                    logger.error(f"MongoDB error inserting company '{company}': {e}")
    return companies_ids




def find_similar_company_list(embedding: List[float], companies_collection, similarity_threshold: float = 0.7) -> Optional[Companies]:
    results = companies_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",  # adjust index name if necessary
                "path": "embedding",
                "queryVector": embedding,
                "numCandidates": 10,
                "limit": 10,
            }
        },
        {
            "$project": {
                "similarityScore": {"$meta": "vectorSearchScore"},
                "document": "$$ROOT",
            }
        },
    ])
    results_list = list(results)
    
    return results_list


def format_candidates( candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format a list of candidate companies to a list of dictionaries with the following fields:
    - id: str
    - name: str
    - ticker: str
    - public: bool
    - parent_company: str
    - description: str
    - sector: str
    - similarity_score: float
    """
    candidate_data = []
    for candidate in candidates:
        if isinstance(candidate, dict) and "document" in candidate:
            doc = candidate["document"]
            candidate_data.append({
                "id": str(doc.get("_id", "")),
                "name": doc.get("name", ""),
                "ticker": doc.get("ticker", ""),
                "public": doc.get("public", False),
                "parent_company": doc.get("parent_company", ""),
                "description": doc.get("description", ""),
                "sector": doc.get("sector", ""),
                "similarity_score": candidate.get("similarityScore", 0)
            })
        elif "similarityScore" in candidate:
            candidate_data.append({
                "id": str(candidate.get("document", {}).get("_id", "")),
                "name": candidate.get("document", {}).get("name", ""),
                "ticker": candidate.get("document", {}).get("ticker", ""),
                "public": candidate.get("document", {}).get("public", False),
                "parent_company": candidate.get("document", {}).get("parent_company", ""),
                "description": candidate.get("document", {}).get("description", ""),
                "sector": candidate.get("document", {}).get("sector", ""),
                "similarity_score": candidate.get("similarityScore", 0)
            })
    return candidate_data


def intruments_to_companies_ids_parallel(
    instruments: List[str],
    companies_collection,
    *,
    return_new_company_ids: bool = False,
) -> Any:
    """
    instruments = ['Datadog', 'MongoDB', 'Snowflake']
    companies_collection = get_mongo_collection(db_name="alphasync_db_dev", collection_name="companies")
    Convert a list of instrument names to a list of company IDs.
    """
    logger.info(f"Iniciando processamento paralelo para {len(instruments)} empresas")

    # Verificar lista vazia
    if not instruments:
        logger.warning("Lista de instrumentos vazia")
        return {}

    # Obter embeddings para todas as empresas em paralelo
    logger.info("Gerando embeddings para as empresas...")
    companies_embeddings = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        companies_embeddings = list(executor.map(get_embedding, instruments))
    logger.info(f"Embeddings gerados com sucesso para {len(companies_embeddings)} empresas")
    
    # Encontrar empresas correspondentes em paralelo
    logger.info("Buscando correspondências existentes no banco de dados...")
    corresponding_companies = []
    find_candidates = lambda x: find_similar_company_list(x, companies_collection)

    with ThreadPoolExecutor(max_workers=10) as executor:
        corresponding_companies = list(executor.map(find_candidates, companies_embeddings))
    
    # Associar instrumentos com IDs existentes ou marcar para processamento
    logger.info("Identificando empresas existentes vs. novas...")
    instruments_ids_mapping = {}
    for instrument, candidates in zip(instruments, corresponding_companies):
        if candidates:
            best_candidate = candidates[0]
            instruments_ids_mapping[instrument] = best_candidate.get("_id") if best_candidate.get("similarityScore", 0) > 0.9 else None
            if instruments_ids_mapping[instrument]:
                logger.debug(f"Correspondência encontrada para '{instrument}': ID {instruments_ids_mapping[instrument]}")
            else:
                logger.debug(f"Similaridade insuficiente para '{instrument}', será processado pelo LLM")
        else:
            instruments_ids_mapping[instrument] = None
            logger.debug(f"Nenhum candidato encontrado para '{instrument}', será processado pelo LLM")
    
    # Estatísticas de correspondência
    existing_count = len([id for id in instruments_ids_mapping.values() if id])
    new_count = len([id for id in instruments_ids_mapping.values() if not id])
    logger.info(f"Empresas já existentes no banco: {existing_count}")
    logger.info(f"Empresas a serem processadas pelo LLM: {new_count}")
    
    # Se não houver empresas novas, retornar imediatamente
    if new_count == 0:
        logger.info("Todas as empresas já existem no banco, finalizando processamento")
        if return_new_company_ids:
            return instruments_ids_mapping, []
        return instruments_ids_mapping

    # Preparar instrumentos para processamento LLM
    logger.info("Preparando dados para processamento LLM...")
    instruments_to_process = []
    for instrument, id in instruments_ids_mapping.items():
        if not id:
            try:
                candidates = corresponding_companies[instruments.index(instrument)][:2]
                instruments_to_process.append({"ticker": instrument, "candidates": format_candidates(candidates)})
            except Exception as e:
                logger.error(f"Erro ao preparar dados para '{instrument}': {e}")
    
    # Carregar prompt e preparar requisições
    try:
        logger.info("Carregando prompt para LLM...")
        with open('prompts/ticker_guesser.md', 'r') as file:
            prompt = file.read()
        
        lista_prompt = []
        for instrument in instruments_to_process:
            lista_prompt.append(prompt.format(input_data=instrument))
        logger.info(f"Preparados {len(lista_prompt)} prompts para envio ao LLM")
    except Exception as e:
        logger.error(f"Erro ao carregar ou formatar prompt: {e}")
        return instruments_ids_mapping
    
    # Processar com LLM
    logger.info("Enviando requisições ao LLM...")
    try:
        results = execute_llm_with_threads(
            lista_prompt,
            model_name="gemini-2.0-flash",
            max_tokens=4000,
            timeout=120.0,
            temperature=1,
            max_workers=10
        )
        logger.info(f"Recebidas {len(results)} respostas do LLM")
    except Exception as e:
        logger.error(f"Erro durante execução do LLM: {e}")
        return instruments_ids_mapping
    
    # Extrair JSON das respostas
    logger.info("Extraindo dados JSON das respostas...")
    extracted_companies = []
    successful_extractions = 0
    for i, result in enumerate(results):
        try:
            company_data = json.loads(extract_json_from_content(result))[0]
            extracted_companies.append(company_data)
            successful_extractions += 1
        except Exception as e:
            logger.error(f"Erro ao extrair JSON da resposta {i+1}: {e}")
            # Adicionar objeto vazio para manter índices alinhados
            extracted_companies.append(None)
    
    logger.info(f"Extraídos com sucesso {successful_extractions} de {len(results)} respostas")
    
    # Se nenhuma extração foi bem-sucedida, retornar
    if successful_extractions == 0:
        logger.warning("Nenhuma extração de JSON bem-sucedida, finalizando processamento")
        return instruments_ids_mapping

    # Preparar objetos para inserção
    logger.info("Preparando objetos Companies para inserção...")
    objects_to_insert = []
    objects_with_no_id = [i[0] for i in instruments_ids_mapping.items() if not i[1]]
    logger.info(f"Total de empresas a serem criadas: {len(objects_with_no_id)}")

    objects_processed = 0
    for company, id in instruments_ids_mapping.items():
        if not id:
            try:
                idx = objects_with_no_id.index(company)
                if idx < len(extracted_companies) and extracted_companies[idx]:
                    company_dict = extracted_companies[idx].copy()
                    company_dict.pop("already_exists", None)

                    # --- normalization ---------------------------------------------------
                    company_dict['name'] = normalize_name(company_dict.get('name', company))
                    company_dict['ticker'] = normalize_ticker(company_dict.get('ticker'))

                    company_dict['embedding'] = companies_embeddings[instruments.index(company)]
                    company_dict['created_at'] = datetime.now()

                    company_object = Companies(**company_dict)
                    objects_to_insert.append(company_object)
                    instruments_ids_mapping[company] = company_object.id
                    objects_processed += 1
                    logger.debug(f"Objeto Companies criado para '{company}': ID {company_object.id}")
                else:
                    logger.warning(f"Dados não disponíveis para criar objeto para '{company}'")
            except Exception as e:
                logger.error(f"Erro ao criar objeto Companies para '{company}': {e}")
    
    logger.info(f"Objetos preparados para inserção: {objects_processed}")

    # Upsert no banco de dados (garante unicidade via índice {name,ticker})
    if objects_to_insert:
        logger.info(f"Upserting {len(objects_to_insert)} companies …")

        for comp in objects_to_insert:
            filt = {"name": comp.name, "ticker": comp.ticker}
            try:
                doc = companies_collection.find_one_and_update(
                    filt,
                    {"$setOnInsert": comp.model_dump(by_alias=True)},
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                )
                # map original instrument name (not normalized) to id
                original_name = next(k for k, v in instruments_ids_mapping.items() if v == comp.id)
                instruments_ids_mapping[original_name] = str(doc["_id"])
            except Exception as e:
                logger.error(f"Upsert failed for company '{comp.name}': {e}")

    logger.info("Processamento de empresas concluído com sucesso")

    if return_new_company_ids:
        new_company_ids = [cid for cid in instruments_ids_mapping.values() if cid in [str(obj.id) for obj in objects_to_insert]]
        return instruments_ids_mapping, new_company_ids
    return instruments_ids_mapping

