import logging
from models.chunks import Chunk
from models.infos import Info
from typing import Optional, Any
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
import pymongo

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


def upsert_infos_for_chunks(
    relevant_chunks: List[Chunk],
    companies_ids: Dict[str, str],
    sources_ids: Dict[str, str],
    *,
    db_name: str = db_name_alphasync,
    max_workers: int = 10,
    return_new_infos: bool = False,
) -> Any:
    """
    • For every relevant chunk:
        – try to find an existing Info (vector search)
        – if found → update it
        – else → create a new Info
    • Bulk-insert new infos, bulk-update existing, mark chunks processed.
    • Return {chunk_id: info_id} mapping.
    """
    infos_col    = get_mongo_collection(db_name=db_name, collection_name="infos")
    chunks_col   = get_mongo_collection(db_name=db_name, collection_name="chunks")

    # 1) find best candidate per chunk (parallel)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        candidates = list(ex.map(
            lambda c: find_similar_info_vector_search(c, infos_col),
            relevant_chunks
        ))

    mapping: Dict[str, str] = {}
    to_upd, to_ins = [], []

    for chunk, best in zip(relevant_chunks, candidates):
        if best:                           # --- update path ---
            mapping[chunk.id] = "already_exists"
            best.chunk_ids.append(chunk.id)

            # merge companies / sources
            best.companiesId = list(set(
                best.companiesId +
                [companies_ids[i] for i in chunk.instrument_ids if i in companies_ids]
            ))
            if chunk.source:
                best.sourcesId = list(set(best.sourcesId + [sources_ids.get(chunk.source, "")]))

            to_upd.append(best)

        else:                              # --- insert path ---
            new_info = Info(
                embedding     = chunk.embedding,
                chunk_ids     = [chunk.id],
                companiesId   = [companies_ids[i] for i in chunk.instrument_ids if i in companies_ids],
                sourcesId     = [sources_ids.get(chunk.source, "")] if chunk.source else [],
                postId        = ""
            )
            to_ins.append(new_info)
            mapping[chunk.id] = new_info.id

    # 2) bulk-persist
    if to_upd:
        infos_col.bulk_write([
            pymongo.UpdateOne(
                {"_id": info.id},
                {"$set": info.model_dump(by_alias=True, exclude={"id"})}
            ) for info in to_upd
        ])

    if to_ins:
        infos_col.insert_many([i.model_dump(by_alias=True) for i in to_ins])

    # 3) tag chunks as processed
    chunks_col.update_many(
        {"_id": {"$in": [c.id for c in relevant_chunks]}},
        {"$set": {"was_processed": True}}
    )

    if return_new_infos:
        return mapping, to_ins
    return mapping
