from models.chunks import Chunk
from models.infos import Info
from typing import Optional


def find_similar_info_vector_search(chunk: Chunk, infos_collection, similarity_threshold: float = 0.98) -> Optional[Info]:
    results = infos_collection.aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index_loop_infos",
                "path": "embedding",
                "queryVector": chunk.embedding,
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
            return Info(**most_similar["document"])
    return None