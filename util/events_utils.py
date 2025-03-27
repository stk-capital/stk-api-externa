
from util.embedding_utils import get_embedding
from typing import List, Dict, Any
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_similar_events(event_text: str, 
                        events_collection, similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
    """
    Find similar events in the database using only vector similarity of event text.
    
    Args:
        event_text: Text describing the event (used for semantic similarity)
        events_collection: MongoDB collection for events
        similarity_threshold: Minimum similarity score for vector matches
    
    Returns:
        List of matching event dictionaries
    """
    try:
        # Generate embedding for the event text
        embedding = get_embedding(event_text)
        
        # Use vector search to find semantically similar events
        vector_results = events_collection.aggregate([
            {
                "$vectorSearch": {
                    "index": "vector_index_loop_events",
                    "path": "embedding",
                    "queryVector": embedding,
                    "numCandidates": 20,
                    "limit": 20,
                }
            },
            {
                "$project": {
                    "similarityScore": {"$meta": "vectorSearchScore"},
                    "document": "$$ROOT",
                }
            },
        ])
        
        # Convert to list and filter by similarity threshold only
        candidates = []
        vector_results_list = list(vector_results)
        
        for result in vector_results_list:
            doc = result["document"]
            similarity_score = result["similarityScore"]
            
            # Only keep results above similarity threshold
            if similarity_score >= similarity_threshold:
                # Convert ObjectId to string for serialization
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                
                # Add similarity score for reference
                doc["_similarity_score"] = similarity_score
                candidates.append(doc)
        
        # Sort by similarity score
        candidates.sort(key=lambda x: x["_similarity_score"], reverse=True)
        
        # Remove metadata fields used for sorting
        events = []
        for candidate in candidates:
            candidate.pop("_similarity_score", None)
            events.append(candidate)
            
        return events
    except Exception as e:
        logger.error(f"Error finding similar events: {e}")
        return []
    
def prepare_event_candidates(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Event objects to the format expected by the event extractor graph.
    
    Args:
        events: List of event dictionaries from the database
    
    Returns:
        List of event candidates formatted for the graph input
    """
    candidates = []
    
    for event in events:
        try:
            # Format dates as ISO strings if present
            date_str = None
            if event.get("date"):
                if isinstance(event["date"], datetime):
                    date_str = event["date"].isoformat()
                elif isinstance(event["date"], str):
                    date_str = event["date"]  # Already a string
            
            candidate = {
                "id": event.get("_id") or event.get("id"),
                "name": event.get("name", ""),
                "description": event.get("description", ""),
                "date": date_str,
                "original_date_text": event.get("original_date_text", ""),
                "location": event.get("location"),
                "event_type": event.get("event_type", ""),
                "companies_ids": event.get("companies_ids", []),
                "confidence": event.get("confidence", 1.0)
            }
            candidates.append(candidate)
        except Exception as e:
            logger.error(f"Error preparing event candidate: {e}")
            continue
            
    return candidates