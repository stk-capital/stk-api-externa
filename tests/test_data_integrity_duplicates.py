import os
import logging
import pytest
from collections import defaultdict
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def mongo_collections():
    """Return required Mongo collections as a dict."""
    return {
        "companies": get_mongo_collection(db_name=db_name_alphasync, collection_name="companies"),
        "users": get_mongo_collection(db_name=db_name_stkfeed, collection_name="users"),
        "chunks": get_mongo_collection(db_name=db_name_alphasync, collection_name="chunks"),
        "infos": get_mongo_collection(db_name=db_name_alphasync, collection_name="infos"),
        "posts": get_mongo_collection(db_name=db_name_stkfeed, collection_name="posts"),
    }


def _build_duplicate_groups(companies_coll):
    """Group company ids by normalized (name, ticker)."""
    pipeline = [
        {
            "$group": {
                "_id": {
                    "name": {"$toLower": "$name"},
                    "ticker": {"$toUpper": "$ticker"},
                },
                "ids": {"$addToSet": "$_id"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]
    return list(companies_coll.aggregate(pipeline))


def test_duplicate_companies(mongo_collections):
    companies_coll = mongo_collections["companies"]
    dups = _build_duplicate_groups(companies_coll)

    if dups:
        logger.warning("Duplicate companies detected: %s", dups)
    assert len(dups) == 0, f"Found {len(dups)} duplicate company groups"


def test_duplicate_users(mongo_collections):
    users_coll = mongo_collections["users"]
    pipeline = [
        {"$match": {"companyId": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$companyId", "ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    dup_users = list(users_coll.aggregate(pipeline))
    if dup_users:
        logger.warning("Companies with multiple users detected: %s", dup_users)
    assert len(dup_users) == 0, f"Found {len(dup_users)} companies with duplicate users"


def test_posts_reference_canonical_user(mongo_collections):
    users_coll = mongo_collections["users"]
    posts_coll = mongo_collections["posts"]

    # build set of valid user ids
    valid_user_ids = {str(u["_id"]) for u in users_coll.find({}, {"_id": 1})}
    invalid_posts = posts_coll.find({"userId": {"$nin": list(valid_user_ids)}}, {"_id": 1, "userId": 1}).limit(10)
    invalid_posts = list(invalid_posts)
    if invalid_posts:
        logger.warning("Posts referencing non-existent users: %s", invalid_posts)
    assert not invalid_posts, f"Found {len(invalid_posts)} posts referencing missing users"


def test_chunks_infos_no_duplicate_company_ref(mongo_collections):
    companies_coll = mongo_collections["companies"]
    chunks_coll = mongo_collections["chunks"]
    infos_coll = mongo_collections["infos"]

    # duplicate company id sets
    dup_groups = _build_duplicate_groups(companies_coll)
    duplicate_ids = {str(cid) for g in dup_groups for cid in g["ids"]}
    if not duplicate_ids:
        pytest.skip("No duplicate companies to test against")

    offending_chunks = chunks_coll.count_documents({"instrument_ids": {"$in": list(duplicate_ids)}})
    offending_infos = infos_coll.count_documents({"companiesId": {"$in": list(duplicate_ids)}})

    if offending_chunks or offending_infos:
        logger.warning("Chunks referencing duplicate companies: %s", offending_chunks)
        logger.warning("Infos referencing duplicate companies: %s", offending_infos)
    assert offending_chunks == 0 and offending_infos == 0, "Chunks/Infos reference duplicate company ids" 