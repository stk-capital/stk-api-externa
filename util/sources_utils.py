
from models.sources import Source
from typing import List, Optional


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
