import logging
from models.chunks import Chunk
from models.infos import Info
from typing import Optional

logger = logging.getLogger(__name__)

def find_similar_info_vector_search(chunk: Chunk, infos_collection, similarity_threshold: float = 0.98) -> Optional[Info]:
    # Evitar consulta vetorial sem embedding válido
    embedding = getattr(chunk, 'embedding', None)
    if not embedding or (hasattr(embedding, '__len__') and len(embedding) == 0):
        logger.warning(f"[INFOS-SEARCH] Embedding ausente ou vazio para chunk {getattr(chunk, 'id', '')}, pulando vector search")
        return None
    
    results = infos_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index_loop_infos",
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
        if most_similar.get("similarityScore", 0) >= similarity_threshold:
            return Info(**most_similar["document"])
    return None