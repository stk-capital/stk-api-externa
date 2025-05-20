from models.sources import Source
from typing import List, Optional
from util.embedding_utils import get_embedding
from util.mongodb_utils import get_mongo_collection
from util.parsing_utils import extract_json_from_content


from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def find_similar_source(embedding: List[float], sources_collection, similarity_threshold: float = 0.9) -> Optional[Source]:
    results = sources_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index_loop_sources",  # adjust index name if necessary
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
            return Source(**most_similar["document"])
    return None


def intruments_to_sources_ids_parallel(instruments: List[str], sources_collection) -> Dict[str, Any]:
    """
    Convert a list of instrument names to a list of company IDs.
    """
    logger.info(f"Iniciando processamento paralelo para {len(instruments)} fontes")

    # Verificar lista vazia
    if not instruments:
        logger.warning("Lista de instrumentos vazia")
        return {}

    # Obter embeddings para todas as fontes em paralelo
    logger.info("Gerando embeddings para as fontes...")
    sources_embeddings = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        sources_embeddings = list(executor.map(get_embedding, instruments))
    logger.info(f"Embeddings gerados com sucesso para {len(sources_embeddings)} fontes")
    
    # Encontrar fontes correspondentes em paralelo
    logger.info("Buscando correspondências existentes no banco de dados...")
    corresponding_sources = []
    find_candidates = lambda x: find_similar_source(x, sources_collection)

    with ThreadPoolExecutor(max_workers=10) as executor:
        corresponding_sources = list(executor.map(find_candidates, sources_embeddings))
    
    # Associar instrumentos com IDs existentes ou criar novos
    objects_to_insert_list = []
    instruments_ids_mapping = {}
    
    # Contador de correspondências encontradas
    matches_found = 0
    new_sources = 0
    
    logger.info("Processando resultados e preparando inserções...")
    for instrument, best_candidate in zip(instruments, corresponding_sources):
        if best_candidate:
            instruments_ids_mapping[instrument] = best_candidate.id
            matches_found += 1
            logger.debug(f"Correspondência encontrada para '{instrument}': ID {best_candidate.id}")
        else:
            # Criar nova fonte
            new_source = Source(name=instrument, embedding=sources_embeddings[instruments.index(instrument)])
            objects_to_insert_list.append(new_source)
            instruments_ids_mapping[instrument] = new_source.id
            new_sources += 1
            logger.debug(f"Nova fonte criada para '{instrument}': ID {new_source.id}")

    logger.info(f"Fontes correspondentes encontradas: {matches_found}")
    logger.info(f"Novas fontes a serem inseridas: {new_sources}")

    # Inserir novas fontes no banco de dados
    if objects_to_insert_list:
        try:
            sources_collection = get_mongo_collection("sources", db_name="alphasync_db_dev")
            objects_to_insert_dict = [object.model_dump(by_alias=True) for object in objects_to_insert_list]
            result = sources_collection.insert_many(objects_to_insert_dict)
            logger.info(f"Inseridas {len(result.inserted_ids)} novas fontes no banco de dados")
        except Exception as e:
            logger.error(f"Erro ao inserir fontes no banco: {e}")
    else:
        logger.info("Nenhuma nova fonte para inserir")

    logger.info("Processamento de fontes concluído com sucesso")
    return instruments_ids_mapping