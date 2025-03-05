import asyncio
import datetime
import json
import logging
import re
import uuid
from typing import Any, Dict, List, Optional

import websockets
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from pymongo import MongoClient, errors

# Airflow imports

from datetime import datetime, timedelta

# Environment variables are assumed to be defined in functions.env
import env

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Added imports for email sending
import random
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv


def uuid_str() -> str:
    """Generates a UUID string."""
    return str(uuid.uuid4())


def custom_encoder(obj):
    """
    Recursively encodes Pydantic models into dictionaries,
    keeping datetime objects as datetime instances.
    """
    if isinstance(obj, BaseModel):
        obj_dict = obj.model_dump(mode='python', exclude_none=True, by_alias=True)
        return {k: custom_encoder(v) for k, v in obj_dict.items()}
    elif isinstance(obj, dict):
        return {k: custom_encoder(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [custom_encoder(v) for v in obj]
    else:
        return obj


# -------------------------------
# Pydantic Models
# -------------------------------
class Attachment(BaseModel):
    filename: str
    size: int
    type: str
    pdf_metadata: Optional[str] = None


class Chunk(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    content: str
    summary: str
    subject: Optional[str] = None
    source: str
    instrument_ids: Optional[List[str]] = None
    embedding: List[float]
    include: bool
    document_id: str
    document_collection: str
    index: int  # Index of the chunk in the document
    published_at: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    was_processed: bool = False  # Flag for processing status

    @property
    def email_id(self) -> str:
        if self.document_collection != "emails":
            raise ValueError(f'source_collection is not "emails": {self.document_collection}')
        return self.document_id

    @email_id.setter
    def email_id(self, value: str):
        self.document_id = value
        self.document_collection = "emails"


class Email(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    message_id: str = Field(default_factory=uuid_str)
    conversation_id: str = Field(default_factory=uuid_str)
    from_address: str
    subject: str
    body: str
    received_at: datetime = Field(default_factory=datetime.now)
    attachments: List[Attachment] = []
    was_processed: bool = False
    relevant: Optional[bool] = None

    @property
    def body_text(self) -> str:
        soup = BeautifulSoup(self.body, "html.parser")
        return soup.get_text(separator=" ", strip=True)

    @property
    def body_pretty(self) -> str:
        soup = BeautifulSoup(self.body, "html.parser")
        return soup.get_text(separator="\n", strip=True)

    def get_lines_pretty(self, numbered: bool = False) -> List[str]:
        lines = self.body_pretty.split("\n")
        if numbered:
            return [f"{i}: {line}" for i, line in enumerate(lines)]
        return lines

    def get_document_pretty(self) -> str:
        lines = self.get_lines_pretty(numbered=True)
        return "\n".join(lines)

    def to_formatted_dict(self, format: str = "html", *args, **kwargs) -> Dict[str, Any]:
        model = custom_encoder(self)
        if format == "html":
            return model
        model.pop("body", None)
        if format == "text":
            model["body_text"] = self.body_text
            return model
        if format == "pretty":
            model["body_pretty"] = self.body_pretty
            return model
        raise ValueError(f"Unknown format {format}")


class Info(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    embedding: List[float]
    chunk_ids: List[str]
    created_at: datetime = Field(default_factory=datetime.now)
    last_updated: datetime = Field(default_factory=datetime.now)
    # Fields for associating companies and sources
    companiesId: List[str] = Field(default_factory=list)
    sourcesId: List[str] = Field(default_factory=list)


class Companies(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str
    ticker: str
    public: bool
    parent_company: str
    description: str  # Existing field
    sector: Optional[str] = None  # New sector field
    embedding: List[float]
    created_at: datetime = Field(default_factory=datetime.now)


class Source(BaseModel):
    id: str = Field(default_factory=uuid_str, alias="_id")
    name: str
    embedding: List[float]
    created_at: datetime = Field(default_factory=datetime.now)


class User(BaseModel):
    companyId: str  # Reference to Companies collection
    name: str
    handle: str
    avatar: str = "/placeholder.svg?height=400&width=400"
    description: str
    website: str
    followers: str = "0"
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


class Post(BaseModel):
    infoId: str  # Reference to Info collection
    userId: str  # Reference to User collection
    source: str
    title: str
    content: str
    timestamp: str
    avatar: str = "/placeholder.svg?height=40&width=40"
    likes: int = 0
    dislikes: int = 0
    shares: int = 0
    created_at: datetime = Field(default_factory=datetime.now)

    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


# -------------------------------
# Database & Email Functions
# -------------------------------
def get_mongo_collection(db_name: str = "alphasync_db", collection_name: str = "emails"):
    """Establish a connection to the MongoDB collection."""
    mongo_uri = env.MONGO_DB_URL
    client = MongoClient(mongo_uri)
    db = client[db_name]
    return db[collection_name]


def get_last_n_emails(n: int = 10) -> List[Email]:
    """
    Retrieve the last n emails via Outlook and insert new ones into MongoDB.
    """
    try:
        from outlook_email import get_recent_emails
        emails_data = get_recent_emails(top_n=n)
        collection = get_mongo_collection()
        email_objects = []

        for email_data in emails_data:
            email_obj = Email(
                message_id=email_data["id"],
                from_address=email_data["from"],
                subject=email_data["subject"],
                body=email_data["body"],
                received_at=email_data["receivedDateTime"],
                attachments=[],
            )
            email_objects.append(email_obj)
            if not collection.find_one({"message_id": email_obj.message_id}):
                try:
                    collection.insert_one(email_obj.to_formatted_dict(by_alias=True))
                    logger.info(f"Inserted email with Message-ID: {email_obj.id}")
                except errors.PyMongoError as e:
                    logger.error(f"Failed to insert email into MongoDB: {e}")
            else:
                logger.info(f"Email with Message-ID: {email_obj.id} already exists")
        return email_objects
    except Exception as e:
        logger.error(f"Failed to retrieve emails: {e}")
        raise


async def connect_to_graph_execution(
    graph_id: str, initial_message: str, timeout_seconds: float = 30,
    retry_attempts: int = 5
) -> List[Dict[str, Any]]:
    uri = f"ws://localhost:8000/api/graphs/{graph_id}/execute"
    attempt = 0
    while attempt < retry_attempts:
        try:
            responses = []
            async with websockets.connect(uri, ping_interval=None) as websocket:
                request_payload = json.dumps({"initial_message": initial_message})
                logger.info(f"Sending graph API request: {request_payload}")
                await websocket.send(request_payload)
                while True:
                    response = await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds)
                    logger.info(f"Received: {response}")
                    response_dict = json.loads(response)
                    responses.append(response_dict)
                    if response_dict.get("status") == "completed":
                        break
                return responses
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
            attempt += 1
            logger.error(f"Graph execution attempt {attempt} failed with error: {e}")
            if attempt >= retry_attempts:
                raise
            await asyncio.sleep(2)
        except Exception as e:
            attempt += 1
            logger.error(f"Graph execution attempt {attempt} failed with error: {e}")
            if attempt >= retry_attempts:
                raise
            await asyncio.sleep(1)


def get_unprocessed_emails() -> List[Email]:
    """Retrieve unprocessed emails from MongoDB."""
    collection = get_mongo_collection()
    try:
        query = {
            "$and": [
                {"$or": [{"was_processed": False}, {"was_processed": {"$exists": False}}]},
                {"$or": [{"relevant": {"$ne": False}}, {"relevant": {"$exists": False}}]}
            ]
        }
        cursor = collection.find(query)
        emails = []
        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            if "received_at" in doc and isinstance(doc["received_at"], str):
                doc["received_at"] = datetime.fromisoformat(doc["received_at"])
            if "attachments" in doc:
                doc["attachments"] = [Attachment(**att) for att in doc["attachments"]]
            email_obj = Email(**doc)
            emails.append(email_obj)
        return emails
    except Exception as e:
        logger.error(f"Failed to retrieve unprocessed emails: {e}")
        raise


def extract_brace_arguments(text: str) -> Dict[str, Any]:
    """
    Extract key-value pairs from text enclosed in double braces.
    """
    pattern = r"\{\{(.*?)\}\}"
    matches = re.findall(pattern, text, re.DOTALL)
    extracted = {}
    for match in matches:
        try:
            key, value = match.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith("[") or value.startswith("{"):
                value = value.replace("\n", "").replace("\r", "").strip()
                try:
                    extracted[key] = json.loads(value)
                except json.JSONDecodeError:
                    extracted[key] = value
            else:
                extracted[key] = value
        except ValueError:
            continue
    return extracted


def filter_emails():
    """
    Process unprocessed emails by running them through a graph execution,
    then update the email document in MongoDB.
    """
    emails_list = get_unprocessed_emails()
    collection = get_mongo_collection()
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        try:
            response = asyncio.run(
                connect_to_graph_execution(
                    "66e88c9c7d27c163b1c128f2", initial_message=email_obj.get_document_pretty()
                )
            )
            content = response[0]["step"]["Email Filter"][-1]["content"]
            email_obj.relevant = extract_brace_arguments(content).get("relevant", "false") == "true"
        except (IndexError, KeyError) as e:
            logger.error(f"Failed to extract relevant data: {e}")
        email_obj.was_processed = False
        try:
            collection.update_one(
                {"_id": email_obj.id},
                {"$set": {"was_processed": email_obj.was_processed, "relevant": email_obj.relevant}},
            )
            logger.info(f"Updated email with ID: {email_obj.id}")
        except errors.PyMongoError as e:
            logger.error(f"MongoDB update error: {e}")


# def get_relevant_unprocessed_emails() -> List[Email]:
#     """Retrieve unprocessed emails marked as relevant."""
#     collection = get_mongo_collection()
#     query = {"relevant": True, "was_processed": False}
#     cursor = collection.find(query)
#     emails = []
#     for doc in cursor:
#         if "_id" in doc:
#             doc["_id"] = str(doc["_id"])
#         if "received_at" in doc and isinstance(doc["received_at"], str):
#             doc["received_at"] = datetime.fromisoformat(doc["received_at"])
#         if "attachments" in doc:
#             doc["attachments"] = [Attachment(**att) for att in doc["attachments"]]
#         emails.append(Email(**doc))
#     return emails


def extract_json_from_content(content: str) -> str:
    """
    Extract JSON string from content. If the content is already a valid JSON string,
    return it as it is, otherwise look for content delimited by ```json and ```.
    """
    try:
        # Try to directly parse the content to check if it's valid JSON.
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass

    pattern = r"```json\s*(.*?)\s*```"
    match = re.search(pattern, content, re.DOTALL)
    if match:
        json_str = match.group(1)
        return json_str.replace('\\n', '\n')
    else:
        raise ValueError("JSON content not found.")


def chunkenize_emails():
    """
    Process relevant unprocessed emails to generate and store chunks.
    """
    emails_list = get_unprocessed_emails()
    emails_collection = get_mongo_collection(collection_name="emails")
    chunks_collection = get_mongo_collection(collection_name="chunks")

    # Delete existing chunks for these emails
    for email_obj in emails_list:
        chunks_collection.delete_many({"document_id": email_obj.id})

    graph_id = "66e9bc0d68d9def3e3bd49b6"
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        try:
            response = asyncio.run(
                connect_to_graph_execution(
                    graph_id, initial_message=email_obj.get_document_pretty()
                )
            )
            json_string = response[0]["step"]["Email Chunckenizer"][-1]["content"]
            content = extract_json_from_content(json_string)
            chunk_data = json.loads(content)["chunks"]
        except (IndexError, KeyError, ValueError, json.JSONDecodeError) as e:
            logger.error(f"Chunk extraction failed: {e}")
            continue

        email_lines = email_obj.get_lines_pretty(numbered=False)
        previous_end = -1
        for chunk_index, chunk in enumerate(chunk_data):
            current_end = chunk["end"]
            start_index = max(previous_end + 1, 0)
            end_index = min(current_end + 1, len(email_lines))
            chunk_text = "\n".join(email_lines[start_index:end_index])
            previous_end = current_end

            try:
                embedding = get_embedding(chunk_text)
            except Exception as e:
                logger.error(f"Embedding generation failed: {e}")
                embedding = []

            chunk_obj = Chunk(
                content=chunk_text,
                summary=chunk["summary"],
                subject=chunk["subject"],
                source=chunk["source"],
                instrument_ids=chunk.get("companies", []),
                embedding=embedding,
                include=chunk["relevant"],
                index=chunk_index,
                document_id=email_obj.id,
                document_collection="emails",
                published_at=email_obj.received_at,
                created_at=datetime.now(),
            )
            email_obj.was_processed = True
            try:
                emails_collection.update_one(
                    {"_id": email_obj.id},
                    {"$set": {"was_processed": email_obj.was_processed, "relevant": email_obj.relevant}},
                )
                logger.info(f"Email updated with ID: {email_obj.id}")
            except errors.PyMongoError as e:
                logger.error(f"Error updating email: {e}")

            try:
                chunks_collection.insert_one(chunk_obj.model_dump(by_alias=True))
                logger.info(f"Inserted chunk with ID: {chunk_obj.id}")
            except errors.PyMongoError as e:
                logger.error(f"Error inserting chunk: {e}")


def find_similar_info_vector_search(chunk: Chunk, infos_collection, similarity_threshold: float = 0.9) -> Optional[Info]:
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


def get_embedding(text: str, timeout_seconds: float = 20, retry_attempts: int = 3) -> List[float]:
    import time
    from openai import OpenAI
    client = OpenAI(api_key=env.OPENAI_API_KEY)
    for attempt in range(retry_attempts):
        try:
            response = client.embeddings.create(
                input=text,
                model="text-embedding-3-small",
                timeout=timeout_seconds
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"OpenAI embedding call failed on attempt {attempt+1} with error: {e}")
            if attempt == retry_attempts - 1:
                raise
            time.sleep(1)


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
    candidate_list = []
    for result in results:
        score = result.get("similarityScore", 0)
        if score > similarity_threshold:
            doc = result["document"]
            candidate = {
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


# --- End New Functions ---


def _process_emails(n: int = 10):
    """
    End-to-end email processing: retrieve emails, filter, chunk, and prepare for info association.
    """
    get_last_n_emails(n=10)
    filter_emails()
    chunkenize_emails()


def parse_companies(content: str) -> List[Dict[str, Any]]:
    cleaned = extract_json_from_content(content).strip()
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"Company parsing error: {e}")
        return []


def _process_chunks():
    """
    Process new chunks by:
      1. Determining associated company and source IDs (existing or new),
      2. Finding an existing Info document via vector search using the chunk's embedding,
         and updating it with the new chunk and associated company/source IDs,
         or creating a new Info if none exists.
      3. Marking the chunk as processed.
    """
    infos_collection = get_mongo_collection(collection_name="infos")
    chunks_collection = get_mongo_collection(collection_name="chunks")
    companies_collection = get_mongo_collection(collection_name="companies")
    sources_collection = get_mongo_collection(collection_name="sources")

    # Modified query to process only chunks marked as relevant (include=True)
    query = {"was_processed": False, "include": True}
    cursor = chunks_collection.find(query)
    #grab first doc for debugging, its not a list,s o i cant use cursor[0], but it comes from cursor
    if cursor:
        for doc in cursor:
            #doc = list(cursor)[0]
            try:
                chunk = Chunk(**doc)
            except Exception as e:
                logger.error(f"Error parsing chunk: {e}")
                continue

            # Process companies from chunk.instrument_ids
            companies_ids = []
            if chunk.instrument_ids != []:
                for company in chunk.instrument_ids:
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
                            result = companies_collection.insert_one(new_company.model_dump(by_alias=True))
                            new_company.id = result.inserted_id if result.inserted_id else new_company.id
                            companies_ids.append(new_company.id)
                            logger.info(f"Inserted new company '{company}' with ID: {new_company.id}")
                        except errors.PyMongoError as e:
                            logger.error(f"MongoDB error inserting company '{company}': {e}")

            # Process source from chunk.source (assumed to be a single string)
            sources_ids = []
            if chunk.source != "":
                try:
                    source_embedding = get_embedding(chunk.source)
                except Exception as e:
                    logger.error(f"Error generating embedding for source '{chunk.source}': {e}")
                    source_embedding = []
                if source_embedding:
                    existing_source = find_similar_source(source_embedding, sources_collection)
                    if existing_source:
                        sources_ids.append(existing_source.id)
                    else:
                        new_source = Source(
                            name=chunk.source,
                            embedding=source_embedding,
                            created_at=datetime.now(),
                        )
                        try:
                            result = sources_collection.insert_one(new_source.model_dump(by_alias=True))
                            new_source.id = result.inserted_id if result.inserted_id else new_source.id
                            sources_ids.append(new_source.id)
                            logger.info(f"Inserted new source '{chunk.source}' with ID: {new_source.id}")
                        except errors.PyMongoError as e:
                            logger.error(f"MongoDB error inserting source '{chunk.source}': {e}")

            # Find an existing Info document using vector search with chunk.embedding
            similar_info = find_similar_info_vector_search(chunk, infos_collection)
            if similar_info:
                update_ops = {}
                if chunk.id not in similar_info.chunk_ids:
                    update_ops.setdefault("$addToSet", {})["chunk_ids"] = chunk.id
                if companies_ids:
                    update_ops.setdefault("$addToSet", {})["companiesId"] = {"$each": companies_ids}
                if sources_ids:
                    update_ops.setdefault("$addToSet", {})["sourcesId"] = {"$each": sources_ids}
                if update_ops:
                    update_ops["$set"] = {"last_updated": datetime.now()}
                    try:
                        infos_collection.update_one({"_id": similar_info.id}, update_ops)
                        logger.info(f"Updated info '{similar_info.id}' with chunk '{chunk.id}'")
                    except errors.PyMongoError as e:
                        logger.error(f"MongoDB error updating info '{similar_info.id}': {e}")
            else:
                # Create a new Info document with the current chunk as the head
                new_info = Info(
                    embedding=chunk.embedding,
                    chunk_ids=[chunk.id],
                    companiesId=companies_ids,
                    sourcesId=sources_ids,
                    created_at=datetime.now(),
                    last_updated=datetime.now(),
                )
                try:
                    infos_collection.insert_one(new_info.model_dump(by_alias=True))
                    logger.info(f"Created new info for chunk '{chunk.id}'")
                except errors.PyMongoError as e:
                    logger.error(f"MongoDB error inserting new info for chunk '{chunk.id}': {e}")

            # Mark chunk as processed
            try:
                chunks_collection.update_one({"_id": chunk.id}, {"$set": {"was_processed": True}})
            except errors.PyMongoError as e:
                logger.error(f"Error marking chunk '{chunk.id}' as processed: {e}")


def _log_processing_summary(run_start_time: datetime):
    """
    Enhanced logging to include STKFeed metrics
    """
    emails_collection = get_mongo_collection("alphasync_db", "emails")
    chunks_collection = get_mongo_collection("alphasync_db", "chunks")
    companies_collection = get_mongo_collection("alphasync_db", "companies")
    sources_collection = get_mongo_collection("alphasync_db", "sources")
    infos_collection = get_mongo_collection("alphasync_db", "infos")
    users_collection = get_mongo_collection("STKFeed", "users")
    posts_collection = get_mongo_collection("STKFeed", "posts")

    # Existing counts
    num_important_chunks = chunks_collection.count_documents({
        "include": True,
        "created_at": {"$gte": run_start_time},
    })
    num_total_chunks = chunks_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_relevant_emails = emails_collection.count_documents({
        "relevant": True,
        "received_at": {"$gte": run_start_time},
    })
    num_companies = companies_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_sources = sources_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_info_docs = infos_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    
    # New STKFeed metrics
    num_users = users_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })
    num_posts = posts_collection.count_documents({
        "created_at": {"$gte": run_start_time},
    })

    logger.info("----- Processing Summary -----")
    logger.info("[AlphaSync DB]")
    logger.info("Relevant emails: %d", num_relevant_emails)
    logger.info("Important chunks: %d/%d", num_important_chunks, num_total_chunks)
    logger.info("Companies created: %d", num_companies)
    logger.info("Sources created: %d", num_sources)
    logger.info("Info documents: %d", num_info_docs)
    
    logger.info("[STKFeed DB]")
    logger.info("Users created: %d", num_users)
    logger.info("Posts created: %d", num_posts)
    logger.info("-------------------------------")


def _process_feed():
    """
    Process feed integrations:
    1. Create users for new companies
    2. Create posts for new infos
    """
    _create_users_from_companies()
    _create_posts_from_infos()


def _create_users_from_companies():
    """Create STKFeed users for new companies in alphasync_db"""
    companies_coll = get_mongo_collection("alphasync_db", "companies")
    users_coll = get_mongo_collection("STKFeed", "users")
    
    # Find companies without existing users
    pipeline = [
        {"$lookup": {
            "from": "STKFeed.users",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "existing_users"
        }},
        {"$match": {"existing_users": {"$size": 0}}}
    ]
    
    for company in companies_coll.aggregate(pipeline):
        try:
            # Check for existing user by multiple criteria
            existing_user = users_coll.find_one({
                "$or": [
                    {"companyId": str(company['_id'])},
                    {"name": {"$regex": f"^{re.escape(company['name'])}$", "$options": "i"}},
                    {"handle": {"$regex": f"^{re.escape(company['name'].lower())}$", "$options": "i"}}
                ]
            })
            
            if existing_user:
                logger.info(f"User already exists for company {company['name']}")
                continue

            # Handle generation and uniqueness check
            clean_handle = re.sub(r'[^a-zA-Z0-9]', '', company['name']).lower()[:15]
            final_handle = clean_handle
            suffix = 1
            
            while users_coll.find_one({"handle": final_handle}):
                final_handle = f"{clean_handle}{suffix}"
                suffix += 1

            # Create and insert user
            user = User(
                companyId=str(company['_id']),
                name=company['name'],
                handle=final_handle,
                description=company.get('description', ''),
                website=company.get('website', ''),
                followers=_format_followers(company.get('followers', 0))
            )
            
            users_coll.insert_one(user.model_dump(by_alias=True))
            logger.info(f"Created user {final_handle} for company {company['_id']}")

        except Exception as e:
            logger.error(f"Failed creating user for company {company['_id']}: {e}")


def send_notification_email(posts_created, destination_email="ruhany.aragao@gmail.com"):
    """
    Envia um email de notificação quando novos posts forem criados pelo pipeline.
    
    Args:
        posts_created: Lista de posts criados
        destination_email: Email destinatário
    """
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        smtp_username = os.getenv("GMAIL_USER")
        smtp_password = os.getenv("GMAIL_PASSWORD")
        
        if not smtp_username or not smtp_password:
            logger.error("Configurações de Gmail ausentes. Não é possível enviar email.")
            return False
            
        # Criando a mensagem
        msg = MIMEMultipart()
        msg['From'] = smtp_username
        msg['To'] = destination_email
        msg['Subject'] = f"STK API: {len(posts_created)} novos posts criados pelo pipeline"
        
        # Corpo do email
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .post {{ border: 1px solid #ccc; padding: 10px; margin-bottom: 15px; }}
                .title {{ font-weight: bold; color: #333; }}
                .content {{ color: #555; }}
                .source {{ color: #888; font-style: italic; }}
            </style>
        </head>
        <body>
            <h2>Novos Posts Criados</h2>
            <p>O pipeline de processamento de emails acabou de criar {len(posts_created)} novos posts:</p>
        """
        
        # Adiciona até 5 posts na mensagem
        for i, post in enumerate(posts_created[:5]):
            body += f"""
            <div class="post">
                <div class="title">{post.get('title', 'Sem título')}</div>
                <div class="content">{post.get('content', 'Sem conteúdo')[:150]}...</div>
                <div class="source">Fonte: {post.get('source', 'Desconhecida')}</div>
            </div>
            """
        
        if len(posts_created) > 5:
            body += f"<p>E mais {len(posts_created) - 5} posts foram criados...</p>"
        
        body += """
            <p>Este é um email automatizado enviado pelo sistema de pipeline da STK API.</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Conectando e enviando o email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            
        logger.info(f"Email de notificação enviado para {destination_email}")
        return True
            
    except Exception as e:
        logger.error(f"Erro ao enviar email de notificação: {str(e)}")
        return False


def _create_posts_from_infos():
    """Create STKFeed posts for new infos in alphasync_db"""
    from bson import ObjectId  # Add explicit import for ObjectId
    
    infos_coll = get_mongo_collection("alphasync_db", "infos")
    posts_coll = get_mongo_collection("STKFeed", "posts")
    users_coll = get_mongo_collection("STKFeed", "users")
    sources_coll = get_mongo_collection("alphasync_db", "sources")
    chunks_coll = get_mongo_collection("alphasync_db", "chunks")
    
    # Para controle de novos posts criados
    new_posts_created = []
    
    # Create a unique index to prevent duplicate posts
    try:
        posts_coll.create_index([("infoId", 1), ("userId", 1)], unique=True)
        logger.info("Ensured unique index on posts collection")
    except Exception as e:
        logger.error(f"Error creating index: {e}")
    
    # Get all existing post infoIds to use in our filter
    existing_info_ids = set()
    for post in posts_coll.find({}, {"infoId": 1}):
        if "infoId" in post:
            existing_info_ids.add(post["infoId"])
    
    # Query to find infos that don't have posts yet and have at least one chunk ID
    query = {
        "_id": {"$nin": [ObjectId(id_str) for id_str in existing_info_ids if ObjectId.is_valid(id_str)]},
        "chunk_ids": {"$exists": True, "$ne": []}
    }
    
    # Process only infos that don't have posts yet
    for info in infos_coll.find(query):
        try:
            # Convert ObjectId to string
            info_id_str = str(info['_id'])
            
            # Get first chunk content for post body - fetch in batch first
            if not info.get('chunk_ids') or len(info['chunk_ids']) == 0:
                logger.warning(f"No chunk IDs found for info {info_id_str}")
                continue
                
            chunk = chunks_coll.find_one({"_id": info['chunk_ids'][0]})
            if not chunk:
                logger.warning(f"Chunk not found for info {info_id_str}")
                continue
            
            # Get source name
            source_name = "Unknown"
            if info.get('sourcesId') and len(info['sourcesId']) > 0:
                try:
                    source_doc = sources_coll.find_one({"_id": info['sourcesId'][0]})
                    source_name = source_doc.get('name', 'Unknown') if source_doc else 'Unknown'
                except Exception as e:
                    logger.error(f"Error fetching source: {e}")
            
            # Get associated companies and their users
            company_ids = [str(company_id) for company_id in info.get('companiesId', [])]
            if not company_ids:
                logger.warning(f"No company IDs found for info {info_id_str}")
                continue
            
            # Find all users for these companies in a single query
            company_users = list(users_coll.find({"companyId": {"$in": company_ids}}))
            if not company_users:
                logger.info(f"No users found for companies in info {info_id_str}")
                continue
            
            # Create a batch of posts to insert
            posts_to_insert = []
            post_data_list = []
            
            for user in company_users:
                user_id_str = str(user['_id'])
                
                # Check if this specific user already has a post for this info
                existing_user_post = posts_coll.find_one({
                    "infoId": info_id_str,
                    "userId": user_id_str
                })
                
                if existing_user_post:
                    logger.info(f"Post already exists for info {info_id_str} and user {user_id_str}")
                    continue
                
                post_title = chunk.get('subject', '')  # Use chunk subject if available
                
                post = Post(
                    infoId=info_id_str,
                    userId=user_id_str,
                    source=source_name,
                    title=post_title if post_title else "Industry Update",
                    content = f"{chunk.get('summary', '')}: \n\n ´´´{chunk.get('content', '')}´´´" 
                    if chunk.get('content') else "Industry update",
                    timestamp=_relative_time(info['created_at'])
                )
                
                # Add created_at from info to prevent duplicate timing issues
                post.created_at = info['created_at']
                
                post_dict = post.model_dump(by_alias=True)
                posts_to_insert.append(post_dict)
                post_data_list.append(post_dict.copy())
            
            # Bulk insert posts if there are any
            if posts_to_insert:
                try:
                    result = posts_coll.insert_many(posts_to_insert, ordered=False)
                    logger.info(f"Created {len(result.inserted_ids)} posts for info {info_id_str}")
                    
                    # Add IDs to post data and append to new_posts_created
                    for i, post_id in enumerate(result.inserted_ids):
                        post_data_list[i]['_id'] = str(post_id)
                        new_posts_created.append(post_data_list[i])
                        
                except errors.BulkWriteError as bwe:
                    # Handle partial successes
                    successful_inserts = len(posts_to_insert) - len(bwe.details['writeErrors'])
                    logger.warning(f"Bulk insert partially successful: {successful_inserts}/{len(posts_to_insert)} posts created")
                    
                    # If there were any successful inserts, process them
                    if 'insertedIds' in bwe.details:
                        for idx, post_id in bwe.details['insertedIds'].items():
                            idx = int(idx)  # MongoDB returns indices as strings
                            post_data_list[idx]['_id'] = str(post_id)
                            new_posts_created.append(post_data_list[idx])
                            
                except Exception as e:
                    logger.error(f"Error bulk creating posts: {e}")
                
        except Exception as e:
            logger.error(f"Failed processing info {info.get('_id')}: {e}")
    
    # Envia email de notificação se novos posts foram criados
    if new_posts_created:
        logger.info(f"Enviando email de notificação para {len(new_posts_created)} novos posts criados")
        send_notification_email(new_posts_created)
    
    return len(new_posts_created)


def _relative_time(created_at: datetime) -> str:
    """Convert datetime to relative time string"""
    delta = datetime.now() - created_at
    if delta < timedelta(minutes=1):
        return "Just now"
    elif delta < timedelta(hours=1):
        return f"{delta.seconds//60}m"
    elif delta < timedelta(days=1):
        return f"{delta.seconds//3600}h"
    return f"{delta.days}d"


def _format_followers(count: int) -> str:
    """Format follower count for display"""
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    elif count >= 1000:
        return f"{count/1000:.1f}K"
    return str(count)


# -------------------------------
# Airflow DAG Definition
# -------------------------------

def delete_documents_after_date():
    # Set the cutoff date (February 28, 2025)
    cutoff_date = datetime(2025, 2, 28, 23, 59, 59)
    
    # Get the collections
    emails_coll = get_mongo_collection("alphasync_db", "emails")
    chunks_coll = get_mongo_collection("alphasync_db", "chunks")
    
    # Delete emails created after the cutoff date
    email_result = emails_coll.delete_many({"created_at": {"$gt": cutoff_date}})
    print(f"Deleted {email_result.deleted_count} emails created after Feb 28, 2025")
    
    # Delete chunks created after the cutoff date
    chunk_result = chunks_coll.delete_many({"created_at": {"$gt": cutoff_date}})
    print(f"Deleted {chunk_result.deleted_count} chunks created after Feb 28, 2025")


def process_full_pipeline(process_emails_count: int = 10) -> dict:
    """
    Executa o pipeline completo de processamento de emails em sequência:
    1. Processa emails
    2. Processa chunks
    3. Cria usuários a partir de empresas
    4. Cria posts a partir de informações
    5. Registra resumo de processamento
    
    Args:
        process_emails_count: Número de emails a serem processados
        
    Returns:
        Um dicionário com detalhes do processamento
    """
    start_time = datetime.now()
    results = {}
    
    # Etapa 1: Processar emails
    print("Iniciando processamento de emails...")
    _process_emails(n=process_emails_count)
    results["emails_processed"] = process_emails_count
    
    # Etapa 2: Processar chunks
    print("Iniciando processamento de chunks...")
    chunks_result = _process_chunks()
    results["chunks_processed"] = chunks_result if chunks_result else "Concluído"
    
    # Etapa 3: Criar usuários a partir de empresas
    print("Criando usuários a partir de empresas...")
    users_result = _create_users_from_companies()
    results["users_created"] = users_result if users_result else "Concluído"
    
    # Etapa 4: Criar posts a partir de informações
    print("Criando posts a partir de informações...")
    posts_result = _create_posts_from_infos()
    results["posts_created"] = posts_result if posts_result else "Concluído"
    
    # Etapa 5: Registrar resumo de processamento
    print("Gerando resumo de processamento...")
    _log_processing_summary(start_time)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    results["total_duration_seconds"] = duration
    results["completed_at"] = end_time.isoformat()
    
    print(f"Pipeline completo executado em {duration:.2f} segundos")
    return results

#process full pipeline
def process_full_pipeline(process_emails_count=10):


    _process_emails(process_emails_count)
    _process_chunks()
    _create_users_from_companies()
    _create_posts_from_infos()
    results = _log_processing_summary(datetime.now() - timedelta(minutes=5))

    #delete all emails created after 2025-03-03

    return results





# from pymongo import MongoClient
# _process_emails(50)
# _process_chunks()
# mongo_uri = env.MONGO_DB_URL
# client = MongoClient(mongo_uri)
# db = client["alphasync_db"]

# collections = ["emails", "chunks", "infos", "companies", "sources"]
# for coll in collections:
#     result = db[coll].delete_many({})
#     print(f"Deleted {result.deleted_count} documents from {coll}")

# db2 = client["STKFeed"]
# collections2 = ["users", "posts"]
# for coll in collections2:
#     result = db2[coll].delete_many({})
#     print(f"Deleted {result.deleted_count} documents from {coll}")

#test all functionsads
# _process_emails(200)
# _process_chunks()
# _create_users_from_companies()
# _create_posts_from_infos()
# _log_processing_summary(datetime.now() - timedelta(minutes=5))

