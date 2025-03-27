
from util.parsing_utils import extract_json_from_content
from util.langchain_utils import connect_to_graph_execution
from models.chunks import Chunk
from typing import List, Dict, Any
import json
import asyncio
import logging
from util.dates_utils import is_more_precise_date, normalize_date
from util.events_utils import find_similar_events, prepare_event_candidates
from util.embedding_utils import get_embedding
from models.events import Event
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync
from util.companies_utils import intruments_to_companies_ids

import traceback
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_events_from_chunk(chunk: Chunk, candidates: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Extract events from a chunk using the event extractor graph.
    If candidates are provided, they will be used for deduplication.
    
    Args:
        chunk: The Chunk object containing text to analyze
        candidates: Optional list of candidate events for deduplication
        
    Returns:
        List of extracted event dictionaries
    """
    try:
        # Get the graph ID from environment variables or use a default
        graph_id = "67d19268c1bdf13e15f1c310"
        
        # Construct graph input payload with chunk text and candidates
        payload = {
            "text": chunk.content,
            "subject": chunk.subject,
            "source": chunk.source,
            "candidates": candidates or []
        }
        
        # Add published_at date as reference for relative date resolution
        if chunk.published_at:
            payload["published_at"] = chunk.published_at.isoformat()
            
        # Add company information for context
        if chunk.instrument_ids:
            payload["companies_ids"] = chunk.instrument_ids
        
        # Log payload size for monitoring
        payload_json = json.dumps(payload)
        logger.info(f"Event extraction payload size: {len(payload_json)} bytes")
        
        # Execute graph and get response
        response = asyncio.run(
            connect_to_graph_execution(graph_id, initial_message=payload_json)
        )
        
        # Get the last message content from the appropriate step
        try:
            content = response[0]["step"]["Event Extractor"][-1]["content"]
        except (KeyError, IndexError):
            # Fallback to checking all steps if specific step not found
            for step_name in response[0].get("step", {}):
                if "Event" in step_name and len(response[0]["step"][step_name]) > 0:
                    content = response[0]["step"][step_name][-1]["content"]
                    break
            else:
                raise ValueError("Could not find Event Extractor step in response")
        
        # Extract and parse JSON from content
        json_content = extract_json_from_content(content)
        events_data = json.loads(json_content)
        
        # Handle different response formats
        if isinstance(events_data, dict) and "events" in events_data:
            events = events_data["events"]
        elif isinstance(events_data, list):
            events = events_data
        else:
            logger.warning(f"Unexpected event data format: {type(events_data)}")
            events = []
        
        # Validate and normalize event data
        validated_events = []
        for event in events:
            # Ensure all required fields are present
            if not all(key in event for key in ["name", "description", "original_date_text", "event_type"]):
                logger.warning(f"Skipping event with missing required fields: {event}")
                continue
                
            # Ensure boolean flags are properly set
            event["confirmed"] = bool(event.get("confirmed", True))
            event["already_exists"] = bool(event.get("already_exists", False))
            event["needs_update"] = bool(event.get("needs_update", False))
            
            # Set default confidence if not provided
            if "confidence" not in event:
                event["confidence"] = 1.0
            
            validated_events.append(event)
        
        logger.info(f"Extracted {len(validated_events)} events from chunk {chunk.id}")
        return validated_events
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse event extraction response as JSON: {e}")
        return []
    except ValueError as e:
        logger.error(f"Event extraction value error: {e}")
        return []
    except KeyError as e:
        logger.error(f"Missing key in event extraction response: {e}")
        return []
    except Exception as e:
        logger.error(f"Event extraction failed: {str(e)}")
        return []
    
def merge_event_details(existing_event: Event, new_event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge existing event with new data, preferring more precise information.
    
    Args:
        existing_event: The existing Event object in the database
        new_event_data: Dictionary containing new event data
        
    Returns:
        Dictionary with fields that should be updated
    """
    updates = {}
    
    # Check if new date is more precise
    if (new_event_data.get("date") and 
        is_more_precise_date(new_event_data.get("original_date_text", ""), 
                           existing_event.original_date_text)):
        updates["date"] = new_event_data["date"]
        updates["original_date_text"] = new_event_data["original_date_text"]
        logger.info(f"Updating event date with more precise information: {new_event_data['original_date_text']}")
    
    # Check if new location is provided when existing one is missing or less specific
    if new_event_data.get("location"):
        if not existing_event.location:
            updates["location"] = new_event_data["location"]
            logger.info(f"Adding location to event: {new_event_data['location']}")
        elif len(new_event_data["location"]) > len(existing_event.location):
            # Simple heuristic: longer location string might be more specific
            updates["location"] = new_event_data["location"]
            logger.info(f"Updating event with more detailed location: {new_event_data['location']}")
    
    # Merge descriptions if new information is available
    if new_event_data.get("description"):
        if not existing_event.description:
            updates["description"] = new_event_data["description"]
            logger.info("Adding description to event")
        elif len(new_event_data["description"]) > len(existing_event.description) * 1.2:
            # If new description is significantly longer (20% longer), it might have more information
            updates["description"] = new_event_data["description"]
            logger.info("Updating event with more detailed description")
        elif new_event_data["description"] != existing_event.description and "TBD" in existing_event.description:
            # Replace placeholder descriptions with actual content
            updates["description"] = new_event_data["description"]
            logger.info("Replacing placeholder description with actual content")
    
    # Update event type if it was generic and now is specific
    generic_types = ["other", "general", "unknown"]
    if (new_event_data.get("event_type") and 
        existing_event.event_type.lower() in generic_types and
        new_event_data["event_type"].lower() not in generic_types):
        updates["event_type"] = new_event_data["event_type"]
        logger.info(f"Updating generic event type to specific type: {new_event_data['event_type']}")
    
    # Update confirmed status if event is now confirmed
    if new_event_data.get("confirmed", False) and not existing_event.confirmed:
        updates["confirmed"] = True
        logger.info("Updating event status to confirmed")
    
    # Update confidence if higher
    if new_event_data.get("confidence", 0) > existing_event.confidence:
        updates["confidence"] = new_event_data["confidence"]
        logger.info(f"Updating event confidence from {existing_event.confidence} to {new_event_data['confidence']}")
    
    return updates


def _process_events():
    """
    Process chunks marked with has_events=True that haven't been processed for events yet:
    1. For each chunk, generate embeddings for event detection
    2. Search for similar existing events as candidates
    3. Extract events using the graph with candidates
    4. Create new events or update existing ones based on already_exists and needs_update flags
    5. Link events to companies and chunks
    6. Mark chunks as processed for events
    """
    logger.info("Starting event processing pipeline...")
    start_time = datetime.now()
    
    events_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="events")
    chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")
    # Query chunks with events that haven't been processed for event extraction
    #query = {"has_events": True, "was_processed_events": False}
    #also inclurde chubks with no was_processed_events
    #query = {"has_events": True, "was_processed_events": {"$exists": False}}
    query = {"has_events": True}
    
    # Count total chunks to process for progress reporting
    total_chunks = chunks_collection.count_documents(query)
    logger.info(f"Found {total_chunks} chunks with events content flagged for processing")
    
    if total_chunks == 0:
        logger.info("No event chunks to process - event processing completed")
        return {}
    
    # Initialize statistics counters
    stats = {
        "chunks_processed": 0,
        "failed_chunks": 0,
        "events_extracted": 0,
        "new_events_created": 0,
        "events_updated": 0,
        "events_linked": 0
    }
    
    for chunk_doc in chunks_collection.find(query):
        #chunk_doc = list(chunks_collection.find(query))[-1]
        chunk_start_time = datetime.now()
        chunk_id = chunk_doc.get("_id", "unknown")
        
        try:
            chunk = Chunk(**chunk_doc)
            logger.info(f"Processing chunk {chunk.id} [{stats['chunks_processed']+1}/{total_chunks}]")
            
            # Generate embedding for event search if not present
            #event_embedding = chunk.embedding
            
            # Find potential matching events to use as candidates
            candidate_start_time = datetime.now()
            candidate_events = find_similar_events(
                chunk.content,
                events_collection
            )
            candidate_search_duration = (datetime.now() - candidate_start_time).total_seconds()
            logger.info(f"Found {len(candidate_events)} potential matching events based on semantic similarity in {candidate_search_duration:.2f}s")
            
            # Prepare candidates for the graph
            candidates_for_graph = prepare_event_candidates(candidate_events)
            
            # Extract events with deduplication
            extraction_start_time = datetime.now()
            extracted_events = extract_events_from_chunk(chunk, candidates_for_graph)
            extraction_duration = (datetime.now() - extraction_start_time).total_seconds()
            logger.info(f"Extracted {len(extracted_events)} events in {extraction_duration:.2f}s")
            
            stats["events_extracted"] += len(extracted_events)
            
            # Process each extracted event
            for event_data in extracted_events:
                #event_data = extracted_events[0]
                #print(event_data)
                if event_data.get("already_exists"):
                    # Event exists - handle potential updates
                    event_id = event_data.get("id")
                    if event_id and event_data.get("needs_update"):
                        # Find the existing event
                        existing_event_doc = events_collection.find_one({"_id": event_id})
                        if existing_event_doc:
                            existing_event = Event(**existing_event_doc)
                            
                            # Determine updates based on precision comparison
                            updates = merge_event_details(existing_event, event_data)
                            
                            if updates:
                                # Add chunk to event and update details
                                update_ops = {
                                    "$addToSet": {
                                        "chunk_ids": chunk.id,
                                        "companies_ids": {"$each": chunk.instrument_ids or []}
                                    },
                                    "$set": {
                                        **updates,
                                        "last_updated": datetime.now()
                                    }
                                }
                                events_collection.update_one({"_id": event_id}, update_ops)
                                logger.info(f"Updated event '{existing_event.name}' ({event_id}) with more precise details: {', '.join(updates.keys())}")
                                stats["events_updated"] += 1
                            else:
                                # Event exists but no updates needed, just link the chunk
                                update_ops = {
                                    "$addToSet": {
                                        "chunk_ids": chunk.id, 
                                        "companies_ids": {"$each": chunk.instrument_ids or []}
                                    },
                                    "$set": {"last_updated": datetime.now()}
                                }
                                events_collection.update_one({"_id": event_id}, update_ops)
                                logger.info(f"Linked chunk {chunk.id} to existing event '{event_data['name']}' ({event_id})")
                                stats["events_linked"] += 1
                else:
                    # Create new event
                    creation_start = datetime.now()
                    normalized_date = normalize_date(event_data["date"], chunk.published_at)
                    companies_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
                    companies_ids = intruments_to_companies_ids(event_data.get("companies", []), companies_collection)
                    new_event = Event(
                        name=event_data["name"],
                        description=event_data["description"],
                        date=normalized_date,
                        original_date_text=event_data["original_date_text"] or "",
                        location=event_data.get("location"),
                        event_type=event_data["event_type"],
                        companies_ids=companies_ids or [],
                        chunk_ids=[chunk.id],
                        source=chunk.source,
                        confirmed=event_data["confirmed"],
                        confidence=event_data["confidence"],
                        embedding=get_embedding(event_data["name"] + " " + event_data["description"]),
                        created_at=datetime.now(),
                        last_updated=datetime.now(),
                    )
                    
                    result = events_collection.insert_one(new_event.model_dump(by_alias=True))
                    creation_duration = (datetime.now() - creation_start).total_seconds()
                    date_str = normalized_date.isoformat() if normalized_date else "unknown date"
                    
                    logger.info(f"Created new event: '{event_data['name']}' ({event_data['event_type']}) with date {date_str} in {creation_duration:.2f}s")
                    stats["new_events_created"] += 1
            
            # Mark chunk as processed for events
            chunks_collection.update_one({"_id": chunk.id}, {"$set": {"was_processed_events": True}})
            stats["chunks_processed"] += 1
            
            # Log chunk processing duration
            chunk_duration = (datetime.now() - chunk_start_time).total_seconds()
            logger.info(f"Completed processing chunk {chunk.id} in {chunk_duration:.2f}s")
            
        except Exception as e:
            stats["failed_chunks"] += 1
            logger.error(f"Error processing chunk {chunk_id} for events: {e}")
            logger.error(traceback.format_exc())
            # Don't mark as processed on error to allow retry
    
    # Final statistics
    total_duration = (datetime.now() - start_time).total_seconds()
    stats["processing_time"] = total_duration
    avg_time_per_chunk = total_duration / stats["chunks_processed"] if stats["chunks_processed"] > 0 else 0
    
    # Log detailed event processing summary
    logger.info("="*50)
    logger.info("EVENT PROCESSING SUMMARY")
    logger.info("="*50)
    logger.info(f"Total chunks processed: {stats['chunks_processed']}/{total_chunks}")
    logger.info(f"Failed chunks: {stats['failed_chunks']}")
    logger.info(f"Events extracted: {stats['events_extracted']}")
    logger.info(f"New events created: {stats['new_events_created']}")
    logger.info(f"Existing events updated: {stats['events_updated']}")
    logger.info(f"Chunks linked to existing events: {stats['events_linked']}")
    logger.info(f"Total processing time: {total_duration:.2f}s")
    logger.info(f"Average time per chunk: {avg_time_per_chunk:.2f}s")
    logger.info("="*50)
    
    return stats