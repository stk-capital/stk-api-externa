import re
from util.mongodb_utils import get_mongo_collection
from env import db_name_alphasync, db_name_stkfeed
from models.users import User
import logging
from util.users_utils import format_followers, get_company_logo
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

            # Obter logo da empresa usando Clearbit
            company_avatar = get_company_logo(company['name'], str(company['_id']))
            
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