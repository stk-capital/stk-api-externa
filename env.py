import os
from dotenv import load_dotenv

load_dotenv()   

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")
OUTLOOK_TENANT_ID = os.getenv("OUTLOOK_TENANT_ID")
OUTLOOK_AUTHORITY = os.getenv("OUTLOOK_AUTHORITY")
OUTLOOK_SCOPE = os.getenv("OUTLOOK_SCOPE")
USER_EMAIL = os.getenv("USER_EMAIL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_DEV_MONGO_DB = False
DEVELOPMENT_MODE = False

if USE_DEV_MONGO_DB:
    db_name_alphasync = "alphasync_db_dev"
    db_name_stkfeed = "STKFeed_dev"
else:
    db_name_alphasync = "alphasync_db"
    db_name_stkfeed = "STKFeed"

#