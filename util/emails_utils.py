
from models.emails import Email, Attachment
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync
import logging
from typing import List
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_unprocessed_emails() -> List[Email]:
    """Retrieve unprocessed emails from MongoDB."""
    collection = get_mongo_collection(db_name=db_name_alphasync, collection_name="emails")
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