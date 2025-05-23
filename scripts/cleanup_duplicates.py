#!/usr/bin/env python
"""Cleanup duplicate Companies/Users and fix references (Chunks, Infos, Posts).

Usage examples:
    # Dry-run first duplicate group
    python scripts/cleanup_duplicates.py --limit 1

    # Execute fix for first 3 groups
    python scripts/cleanup_duplicates.py --limit 3 --execute

Options:
    --limit N      Number of duplicate company groups to process (default 1)
    --execute      Apply changes to DB; otherwise dry-run only logs.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from bson import ObjectId
from typing import List, Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from util.mongodb_utils import get_mongo_collection, get_mongo_client
from env import db_name_alphasync, db_name_stkfeed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('cleanup_duplicates')

# Collections ---------------------------------------------------------------
# Use client with extended timeouts to avoid NetworkTimeout during large scans
_client_long = get_mongo_client(timeout_ms=300000, connect_timeout_ms=30000, socket_timeout_ms=300000)

companies_col = _client_long[db_name_alphasync]["companies"]
users_col     = _client_long[db_name_stkfeed]["users"]
chunks_col    = _client_long[db_name_alphasync]["chunks"]
infos_col     = _client_long[db_name_alphasync]["infos"]
posts_col     = _client_long[db_name_stkfeed]["posts"]

PREFIX_AVATAR = "/api/images/tmp/company_logos/"

# Helpers -------------------------------------------------------------------

def list_duplicate_company_groups() -> List[Dict[str, Any]]:
    pipeline = [
        {"$group": {
            "_id": {
                "name": {"$toLower": "$name"},
                "ticker": {"$toUpper": "$ticker"},
            },
            "ids": {"$addToSet": "$_id"},
            "created": {"$addToSet": "$created_at"},
            "count": {"$sum": 1},
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
    ]
    return list(companies_col.aggregate(pipeline))


def pick_canonical_id(ids: List[str]) -> str:
    """Return oldest id according to created_at or lexicographic order."""
    docs = list(companies_col.find({"_id": {"$in": ids}}, {"_id": 1, "created_at": 1}))
    docs.sort(key=lambda d: d.get("created_at", datetime.max))
    return docs[0]["_id"]


def update_array_field(coll, array_field: str, dup_id: str, canonical_id: str, batch_size: int = 500):
    """Replace dup_id with canonical_id in array field using batched updates to avoid timeouts."""

    cursor = coll.find({array_field: dup_id}, {"_id": 1}, no_cursor_timeout=True).batch_size(batch_size)

    batch = []
    for doc in cursor:
        batch.append(doc["_id"])
        if len(batch) >= batch_size:
            _process_batch(coll, array_field, dup_id, canonical_id, batch)
            batch.clear()

    if batch:
        _process_batch(coll, array_field, dup_id, canonical_id, batch)


def _process_batch(coll, field: str, dup_id: str, canon_id: str, id_batch: List[Any]):
    # pull dup id
    coll.update_many({"_id": {"$in": id_batch}}, {"$pull": {field: dup_id}})
    # add canonical id if missing
    coll.update_many({"_id": {"$in": id_batch}, field: {"$ne": canon_id}}, {"$addToSet": {field: canon_id}})


def process_group(group: Dict[str, Any], execute: bool):
    ids: List[str] = [str(i) for i in group["ids"]]
    canonical_id = pick_canonical_id(ids)
    dup_ids = [i for i in ids if i != canonical_id]

    logger.info("Processing company duplicate group '%s' ticker '%s' | canonical=%s | dups=%s",
                group["_id"]["name"], group["_id"]["ticker"], canonical_id, dup_ids)

    # --- USERS -------------------------------------------------------------
    user_cursor = users_col.find({"companyId": {"$in": dup_ids}}, {"_id": 1, "companyId": 1})
    dup_user_ids = [str(u["_id"]) for u in user_cursor]

    if execute and dup_user_ids:
        users_col.update_many({"_id": {"$in": [ObjectId(uid) for uid in dup_user_ids]}}, {"$set": {"companyId": canonical_id}})

    # Determine canonical user (oldest) after reassignment
    users_for_company = list(users_col.find({"companyId": canonical_id}))
    if users_for_company:
        users_for_company.sort(key=lambda u: u.get("created_at", ObjectId(u["_id"]).generation_time))
        canonical_user_id = str(users_for_company[0]["_id"])
        extra_users = [str(u["_id"]) for u in users_for_company[1:]]
    else:
        canonical_user_id = None
        extra_users = []

    # --- POSTS -------------------------------------------------------------
    if canonical_user_id:
        all_dup_user_ids = dup_user_ids + extra_users
        if execute and all_dup_user_ids:
            posts_col.update_many({"userId": {"$in": all_dup_user_ids}}, {"$set": {"userId": canonical_user_id}})

    # Remove posts whose userId no longer exists (clean up)
    invalid_post_count = posts_col.count_documents({"userId": {"$nin": [str(u["_id"]) for u in users_col.find({}, {"_id":1})]}})
    if execute and invalid_post_count:
        posts_col.delete_many({"userId": {"$nin": [str(u["_id"]) for u in users_col.find({}, {"_id":1})]}})
        logger.info("Deleted %s orphan posts", invalid_post_count)

    # Remove duplicate users (after post fix)
    if execute and extra_users:
        users_col.delete_many({"_id": {"$in": [ObjectId(uid) for uid in extra_users]}})

    # --- CHUNKS & INFOS ----------------------------------------------------
    for dup_id in dup_ids:
        if execute:
            update_array_field(chunks_col, "instrument_ids", dup_id, canonical_id)
            update_array_field(infos_col, "companiesId", dup_id, canonical_id)

    # --- DELETE DUP COMPANIES --------------------------------------------
    if execute:
        companies_col.delete_many({"_id": {"$in": dup_ids}})

    logger.info("Group processed (execute=%s)", execute)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup duplicate companies/users")
    parser.add_argument("--limit", type=int, default=1, help="Number of duplicate company groups to process")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default dry-run)")
    args = parser.parse_args()

    duplicate_groups = list_duplicate_company_groups()
    if not duplicate_groups:
        logger.info("No duplicate company groups found – nothing to do.")
        sys.exit(0)

    for group in duplicate_groups[: args.limit]:
        process_group(group, execute=args.execute)

    logger.info("Finished. Run tests again to verify integrity.") 