#!/usr/bin/env python
"""Fixes inconsistent avatar paths in STKFeed users collection.

After adopting the standard path pattern
  /api/images/tmp/company_logos/<companyId>.png
some users created antes dessa mudança ficaram com avatar apontando
para placeholder ou para URL retornada pelo Clearbit.

Este script:
1. Seleciona usuários cujo campo *avatar* não comece pelo prefixo padrão.
2. Para cada usuário:
   • Baixa (ou tenta baixar) o logo atual via get_company_logo – isto criará
     o arquivo em /tmp se existir – mas o valor salvo será sempre o caminho
     padronizado.
   • Atualiza o documento na coleção.
3. Gera um pequeno relatório ao final.

Uso:
    python scripts/fix_user_avatars.py --batch 200
"""

import os
import sys
import argparse
import logging
from bson import ObjectId
from datetime import datetime
from typing import List
from pymongo import UpdateOne

# Ensure project root in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from util.mongodb_utils import get_mongo_collection
from util.users_utils import get_company_logo
from env import db_name_stkfeed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fix_user_avatars')

PREFIX = "/api/images/tmp/company_logos/"


def normalize_avatar_path(user_id: str) -> str:
    """Return standardized avatar path for given user/company id."""
    return f"{PREFIX}{user_id}.png"


def fix_avatars(batch_size: int = 200):
    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")

    query = {"avatar": {"$not": {"$regex": f"^{PREFIX}"}}}
    total = users_coll.count_documents(query)
    if total == 0:
        logger.info("All user avatars are already normalized.")
        return

    logger.info(f"Users to fix: {total}")
    cursor = users_coll.find(query, {"_id": 1, "companyId": 1, "name": 1}).batch_size(batch_size)

    processed = success = failed = 0
    bulk_ops: List = []

    for doc in cursor:
        uid = doc["_id"]
        cid = doc.get("companyId") or str(uid)
        company_name = doc.get("name", "")
        processed += 1

        try:
            # Attempt to download logo (optional – ignore result)
            get_company_logo(company_name, cid)
            new_avatar = normalize_avatar_path(cid)
            bulk_ops.append({"_id": uid, "avatar": new_avatar})
            success += 1
        except Exception as e:
            logger.error(f"Failed to process user {uid}: {e}")
            failed += 1

        # Execute bulk every batch_size docs
        if len(bulk_ops) >= batch_size:
            _bulk_update(users_coll, bulk_ops)
            bulk_ops.clear()

    # final bulk
    if bulk_ops:
        _bulk_update(users_coll, bulk_ops)

    logger.info(
        f"Avatar normalization completed | total: {processed} | success: {success} | failed: {failed}")


def _bulk_update(coll, ops):
    """Helper to run unordered bulk update."""
    if not ops:
        return
    requests = [
        UpdateOne(
            {"_id": op["_id"]},
            {"$set": {"avatar": op["avatar"]}},
            upsert=False,
        )
        for op in ops
    ]

    try:
        coll.bulk_write(requests, ordered=False)
    except Exception as e:
        logger.error(f"Bulk update error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize user avatar paths")
    parser.add_argument("--batch", type=int, default=200, help="Batch size for bulk update")
    args = parser.parse_args()

    start = datetime.now()
    fix_avatars(batch_size=args.batch)
    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Finished in {elapsed:.1f}s") 