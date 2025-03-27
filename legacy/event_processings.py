"""
Event processing module - contains functions for event extraction and deduplication.

This module handles:
1. Extracting events from chunk content
2. Processing chunks with events
3. Deduplicating events with similar content
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import asyncio

from email_processor import logger
from email_processor import get_embedding
from email_processor import connect_to_graph_execution
from email_processor import extract_json_from_content
from email_processor import intruments_to_companies_ids
 
from email_processor import uuid_str
from pydantic import BaseModel, Field
import uuid
import re   
import traceback
import env
from util.mongodb_utils import get_mongo_collection

if env.USE_DEV_MONGO_DB:
    db_name_alphasync = "alphasync_db_dev"
    db_name_stkfeed = "STKFeed_dev"
else:
    db_name_alphasync = "alphasync_db"
    db_name_stkfeed = "STKFeed"


class Event(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str  # The name of the event
    description: str  # Brief description of the event
    date: Optional[datetime] = None  # Normalized date in UTC
    original_date_text: str  # Original text describing the date
    location: Optional[str] = None  # Physical or virtual location
    event_type: str  # Category: earnings_call, investor_conference, etc.
    companies_ids: List[str] = Field(default_factory=list)  # Companies involved
    chunk_ids: List[str] = Field(default_factory=list)  # Source chunks
    source: str  # Source of information
    confirmed: bool = True  # Is this a confirmed event or speculative
    confidence: float = 1.0  # Confidence score (0.0-1.0)
    embedding: List[float]  # For similarity search
    created_at: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)

class Chunk(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    content: str
    summary: str
    subject: Optional[str] = None
    source: str
    instrument_ids: Optional[List[str]] = None
    embedding: List[float]
    include: bool
    has_events: bool
    document_id: str
    document_collection: str
    index: int  # Index of the chunk in the document
    published_at: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    was_processed: bool = False  # Flag for general processing status
    was_processed_events: bool = False  # Flag specifically for event extraction processing

    @property
    def email_id(self) -> str:
        if self.document_collection != "emails":
            raise ValueError(f'source_collection is not "emails": {self.document_collection}')
        return self.document_id

    @email_id.setter
    def email_id(self, value: str):
        self.document_id = value
        self.document_collection = "emails"

def is_more_precise_date(new_date_text: str, old_date_text: str) -> bool:
    """
    Determine if the new date text is more specific/precise than the old one.
    
    Args:
        new_date_text: The new date text to evaluate
        old_date_text: The existing date text to compare against
        
    Returns:
        Boolean indicating whether the new date is more precise
    """
    if not new_date_text or not old_date_text:
        return bool(new_date_text and not old_date_text)
    
    # Define precision patterns from most to least precise
    precision_patterns = [
        # Exact datetime with timezone (2024-03-15T14:30:00+00:00)
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',
        # Exact datetime (March 15, 2024 at 2:30 PM)
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        r'\w+ \d{1,2}, \d{4} at \d{1,2}:\d{2} [AP]M',
        # Exact date with time (March 15, 2024 2:30 PM)
        r'\w+ \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M',
        # Exact date (March 15, 2024)
        r'\d{4}-\d{2}-\d{2}',
        r'\w+ \d{1,2}, \d{4}',
        r'\d{1,2}/\d{1,2}/\d{4}',
        # Month and year (March 2024)
        r'\w+ \d{4}',
        r'\d{2}/\d{4}',
        # Quarter references (Q1 2024)
        r'Q[1-4] \d{4}',
        r'[Qq]uarter \d \d{4}',
        # Year only (2024)
        r'\d{4}',
        # Fiscal year references
        r'[Ff]iscal [Yy]ear \d{4}',
        r'FY\d{2}',
        r'FY \d{4}'
    ]
    
    # Find the precision level of each date text
    new_precision = len(precision_patterns)  # Default to lowest precision
    old_precision = len(precision_patterns)
    
    for i, pattern in enumerate(precision_patterns):
        if re.search(pattern, new_date_text):
            new_precision = i
            break
            
    for i, pattern in enumerate(precision_patterns):
        if re.search(pattern, old_date_text):
            old_precision = i
            break
    
    # Lower index means higher precision
    return new_precision < old_precision

def normalize_date(date_text: str, reference_date: datetime = None) -> Optional[datetime]:
    """
    Convert date text to normalized datetime object, handling various formats.
    
    Args:
        date_text: The text description of the date to normalize
        reference_date: Optional reference date for resolving relative expressions,
                        defaults to current datetime if not provided
    
    Returns:
        Normalized datetime object or None if parsing fails
    """
    if not date_text:
        return None
        
    # Use current date as default reference if none provided
    if reference_date is None:
        reference_date = datetime.now()
    
    try:
        # Clean up the input text
        cleaned_text = date_text.strip().lower()
        
        # Attempt direct parsing for ISO format dates
        try:
            if 'T' in date_text and ('+' in date_text or 'Z' in date_text):
                # ISO format with timezone: 2024-03-15T14:30:00+00:00 or 2024-03-15T14:30:00Z
                return datetime.fromisoformat(date_text.replace('Z', '+00:00'))
            elif 'T' in date_text:
                # ISO format without timezone: 2024-03-15T14:30:00
                return datetime.fromisoformat(date_text)
        except (ValueError, TypeError):
            pass
            
        # Handle fiscal quarters (Q1, Q2, Q3, Q4)
        quarter_match = re.search(r'q([1-4])\s*(\d{4}|\d{2})', cleaned_text)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year_text = quarter_match.group(2)
            year = int(year_text)
            
            # Handle two-digit years
            if len(year_text) == 2:
                current_century = reference_date.year // 100 * 100
                year = current_century + year
                
            # Map quarter to month
            month = (quarter - 1) * 3 + 1
            return datetime(year, month, 1)
        
        # Handle fiscal year references (FY2024, FY 2024, Fiscal Year 2024)
        fiscal_match = re.search(r'(fy|fiscal year)\s*(\d{4}|\d{2})', cleaned_text)
        if fiscal_match:
            year_text = fiscal_match.group(2)
            year = int(year_text)
            
            # Handle two-digit years
            if len(year_text) == 2:
                current_century = reference_date.year // 100 * 100
                year = current_century + year
                
            # Assuming fiscal year starts in January - adjust as needed
            return datetime(year, 1, 1)
        
        # Handle common month and year formats (January 2024, Jan 2024)
        month_year_pattern = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{4})'
        month_year_match = re.search(month_year_pattern, cleaned_text)
        if month_year_match:
            month_name = month_year_match.group(1)
            year = int(month_year_match.group(2))
            
            # Map month name to number
            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            month = month_map.get(month_name[:3].lower(), 1)
            return datetime(year, month, 1)
        
        # Handle full dates (January 15, 2024; 15 January 2024; 01/15/2024)
        # First, try dateutil parser which handles many formats
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_text, default=reference_date)
            # Only return if year was explicitly specified in the text
            if ('20' in date_text or '19' in date_text) and parsed_date.year != reference_date.year:
                return parsed_date
            # If the month or day changed from the reference, it was probably specified
            elif (parsed_date.month != reference_date.month or 
                  parsed_date.day != reference_date.day):
                return parsed_date
        except (ValueError, TypeError, ImportError):
            pass
        
        # Handle relative date expressions
        today_match = re.search(r'today', cleaned_text)
        if today_match:
            return datetime(reference_date.year, reference_date.month, reference_date.day)
        
        tomorrow_match = re.search(r'tomorrow', cleaned_text)
        if tomorrow_match:
            next_day = reference_date + timedelta(days=1)
            return datetime(next_day.year, next_day.month, next_day.day)
        
        yesterday_match = re.search(r'yesterday', cleaned_text)
        if yesterday_match:
            prev_day = reference_date - timedelta(days=1)
            return datetime(prev_day.year, prev_day.month, prev_day.day)
        
        # Handle "next week", "next month", "next year"
        next_match = re.search(r'next\s+(week|month|year)', cleaned_text)
        if next_match:
            unit = next_match.group(1)
            if unit == 'week':
                next_date = reference_date + timedelta(days=7)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'month':
                if reference_date.month == 12:
                    return datetime(reference_date.year + 1, 1, 1)
                else:
                    return datetime(reference_date.year, reference_date.month + 1, 1)
            elif unit == 'year':
                return datetime(reference_date.year + 1, 1, 1)
        
        # Handle X days/weeks/months/years from now
        time_delta_match = re.search(r'(\d+)\s+(day|week|month|year)s?\s+from\s+now', cleaned_text)
        if time_delta_match:
            amount = int(time_delta_match.group(1))
            unit = time_delta_match.group(2)
            
            if unit == 'day':
                next_date = reference_date + timedelta(days=amount)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'week':
                next_date = reference_date + timedelta(days=amount * 7)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'month':
                month = reference_date.month - 1 + amount
                year = reference_date.year + month // 12
                month = month % 12 + 1
                return datetime(year, month, 1)
            elif unit == 'year':
                return datetime(reference_date.year + amount, reference_date.month, 1)
        
        # Extract year if present
        year_match = re.search(r'\b(20\d{2}|19\d{2})\b', cleaned_text)
        if year_match:
            year = int(year_match.group(1))
            # If only year is specified, return January 1st of that year
            return datetime(year, 1, 1)
        
        # If all else fails, log warning and return None
        logger.warning(f"Could not normalize date text: '{date_text}'")
        return None
        
    except Exception as e:
        logger.error(f"Error normalizing date '{date_text}': {e}")
        return None
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