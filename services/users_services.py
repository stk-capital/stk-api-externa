import re
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed
from models.users import User
import logging
from util.users_utils import format_followers, get_company_logo
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from pymongo import errors
from bson import ObjectId

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_users_from_companies():
    """Create STKFeed users for new companies in alphasync_db"""
    companies_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")
    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    
    # Find companies without existing users
    pipeline = [
        {"$lookup": {
            "from": f"{db_name_stkfeed}.users",
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

            # Tentar baixar o logo (gera arquivo em /tmp) mas o avatar salvo
            # sempre referencia o caminho padronizado – isso garante que o
            # front exiba placeholder caso o arquivo não exista.
            _ = get_company_logo(company['name'], str(company['_id']))
            company_avatar = f"/api/images/tmp/company_logos/{company['_id']}.png"
            
            # Create and insert user
            user = User(
                companyId=str(company['_id']),
                name=company['name'],
                handle=final_handle,
                description=company.get('description', ''),
                website=company.get('website', ''),
                followers=format_followers(company.get('followers', 0)),
                avatar=company_avatar  # Usar o avatar obtido
            )
            
            users_coll.insert_one(user.model_dump(by_alias=True))
            logger.info(f"Created user {final_handle} for company {company['_id']} with avatar {company_avatar}")

        except Exception as e:
            logger.error(f"Failed creating user for company {company['_id']}: {e}")

def _process_users():
    create_users_from_companies()

# ----------------------------------------------------------------------------
# New helper to guarantee users for a given set of companyIds
# ----------------------------------------------------------------------------

def get_or_create_users_by_companies(company_ids: List[str]) -> Dict[str, List[str]]:
    """Return mapping {companyId: [userId, ...]} guaranteeing that each company
    in *company_ids* has ao menos um usuário.

    1. Consulta existente em **uma** query.
    2. Cria em *bulk* usuários faltantes utilizando paralelização para gerar
       handle e avatar.
    3. Retorna dicionário com IDs (str) de usuários por empresa.
    """

    # --- Normalize ids ------------------------------------------------------
    # Garantimos que todos os ids sejam strings para evitar falhas de lookup
    # quando alguns vêm como ObjectId e outros já são str.
    company_ids = [str(cid) for cid in company_ids]

    if not company_ids:
        return {}

    users_coll = get_mongo_collection(db_name=db_name_stkfeed, collection_name="users")
    companies_coll = get_mongo_collection(db_name=db_name_alphasync, collection_name="companies")

    # 1) fetch existing users em lote
    existing_cursor = users_coll.find({"companyId": {"$in": company_ids}}, {"_id": 1, "companyId": 1})
    users_by_company: Dict[str, List[str]] = {}
    for doc in existing_cursor:
        cid = doc.get("companyId")
        if cid:
            users_by_company.setdefault(cid, []).append(str(doc["_id"]))

    missing_company_ids = [cid for cid in company_ids if cid not in users_by_company]
    if not missing_company_ids:
        return users_by_company

    # 2) carregar empresas faltantes em lote – suporta ObjectId ou string
    def _to_query_id(val: str):
        """Return both string and ObjectId representation when applicable."""
        # tenta converter para ObjectId se parecer com um ObjectId válido
        if re.fullmatch(r"[0-9a-fA-F]{24}", val):
            try:
                return ObjectId(val)
            except Exception:
                pass
        return val

    query_ids = [_to_query_id(cid) for cid in missing_company_ids]

    company_docs = {str(c["_id"]): c for c in companies_coll.find({"_id": {"$in": query_ids}})}

    def _build_user_doc(cid: str):
        company = company_docs.get(cid)
        if not company:
            logger.warning(f"Company doc not found for id {cid}")
            return None

        clean_handle = re.sub(r'[^a-zA-Z0-9]', '', company['name']).lower()[:15]
        final_handle = clean_handle
        suffix = 1
        while users_coll.find_one({"handle": final_handle}, {"_id": 1}):
            final_handle = f"{clean_handle}{suffix}"
            suffix += 1

        # Tentar baixar o logo (gera arquivo em /tmp) mas o avatar salvo
        # sempre referencia o caminho padronizado – isso garante que o
        # front exiba placeholder caso o arquivo não exista.
        _ = get_company_logo(company['name'], cid)
        company_avatar = f"/api/images/tmp/company_logos/{cid}.png"

        user_obj = User(
            companyId=cid,
            name=company['name'],
            handle=final_handle,
            description=company.get('description', ''),
            website=company.get('website', ''),
            followers=format_followers(company.get('followers', 0)),
            avatar=company_avatar,
        )
        return user_obj

    # 3) criar usuários em paralelo
    with ThreadPoolExecutor(max_workers=10) as ex:
        user_objects = list(filter(None, ex.map(_build_user_doc, missing_company_ids)))

    if user_objects:
        try:
            result = users_coll.insert_many([u.model_dump(by_alias=True) for u in user_objects], ordered=False)
            inserted_ids = result.inserted_ids
        except errors.BulkWriteError as bwe:
            inserted_ids = [err.get("_id") for err in bwe.details.get("writeErrors", []) if err.get("_id")]
        except Exception as e:
            logger.error(f"Failed bulk insert users: {e}")
            inserted_ids = []

        # map back inserted ids to companies (preserve order)
        for user_obj, uid in zip(user_objects, inserted_ids):
            cid = user_obj.companyId
            users_by_company.setdefault(cid, []).append(str(uid))

    return users_by_company