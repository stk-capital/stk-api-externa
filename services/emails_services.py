from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync
from util.langchain_utils import connect_to_graph_execution
from util.parsing_utils import extract_brace_arguments, extract_json_from_content
import logging
import asyncio
from pymongo import errors
from util.emails_utils import get_unprocessed_emails
from models.chunks import Chunk
from util.embedding_utils import get_embedding
from datetime import datetime
import json
from typing import List
from models.emails import Email
from util.outlook_utils import get_recent_emails


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def get_last_n_emails(n: int = 10) -> List[Email]:
    """
    Retrieve the last n emails via Outlook and insert new ones into MongoDB.
    """
    try:
        
        emails_data = get_recent_emails(top_n=n)
        collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
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


def filter_emails():
    """
    Process unprocessed emails by running them through a graph execution,
    then update the email document in MongoDB.
    """
    emails_list = get_unprocessed_emails()
    collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        #limit the email to 130 lines
        email_obj.body = "\n".join(email_obj.body.split("\n")[:130])
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

def chunkenize_emails():
    """
    Process relevant unprocessed emails to generate and store chunks.
    """
    emails_list = get_unprocessed_emails()
    emails_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
    chunks_collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks")

    # Delete existing chunks for these emails
    for email_obj in emails_list:
        chunks_collection.delete_many({"document_id": email_obj.id})

    graph_id = "66e9bc0d68d9def3e3bd49b6"
    for email_obj in emails_list:
        #email_obj = list(emails_list)[0]
        #print(email_obj.get_document_pretty())
        #limit the email to 130 lines
        email_obj.body = "\n".join(email_obj.body.split("\n")[:130])
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
                has_events=chunk["has_events"],
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

def _process_emails(n: int = 10):
    """
    End-to-end email processing: retrieve emails, filter, chunk, and prepare for info association.
    """
    get_last_n_emails(n=10)
    filter_emails()
    chunkenize_emails()
