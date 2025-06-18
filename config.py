import os
#from dotenv import load_dotenv 
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
import logging
from typing import Optional, Dict
from sqlalchemy.sql import text
from urllib.parse import quote_plus
VERSION="5.0.5"
SENDER_EMAIL="nik@luxurymarket.com"
SENDER_PASSWORD="wvug kynd dfhd xrjh"
SENDER_NAME='iconluxurygroup'
GOOGLE_API_KEY='AIzaSyDXfc_kdxa5UX2h9D3WwktefCqdyjHasn8'
# AWS credentials and region
# config.py
SEARCH_PROXY_API_URL = "https://api.thedataproxy.com/v2/proxy/fetch"
BRAND_RULES_URL = "https://raw.githubusercontent.com/iconluxurygroup/legacy-icon-product-api/refs/heads/main/task_settings/brand_settings.json"
AWS_ACCESS_KEY_ID ='AKIA2CUNLEV6V627SWI7'
AWS_SECRET_ACCESS_KEY = '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'
REGION = "us-east-2"
# MSSQL connection settings
DB_CONFIG = {
    "server": "35.172.243.170",
    "database": "luxurymarket_p4",
    "username": "luxurysitescraper",
    "password": "Ftu5675FDG54hjhiuu$",
    "driver": "{ODBC Driver 17 for SQL Server}",
}

S3_CONFIG = {
    "endpoint": "https://s3.us-east-2.amazonaws.com",
    "region": "us-east-2",
    "access_key": "AKIA2CUNLEV6V627SWI7",
    "secret_key": "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",
    "bucket_name": "iconluxurygroup",
    "r2_endpoint": "https://97d91ece470eb7b9aa71ca0c781cfacc.r2.cloudflarestorage.com",
    "r2_access_key": "5547ff7ffb8f3b16a15d6f38322cd8bd",
    "r2_secret_key": "771014b01093eceb212dfea5eec0673842ca4a39456575ca7ff43f768cf42978",
    "r2_account_id": "97d91ece470eb7b9aa71ca0c781cfacc",
    "r2_bucket_name": "iconluxurygroup",
    "r2_custom_domain": "https://iconluxury.shop"
}

DB_PASSWORD=  'Ftu5675FDG54hjhiuu$'

# Existing imports and conn_str
BASE_CONFIG_URL = "https://iconluxury.shop/static_settings/"
# Grok API settings for image processing
GROK_API_KEY = os.getenv('GROK_API_KEY', 'xai-ucA8EcERzruUwHAa1duxYallTxycDumI5n3eVY7EJqhZVD0ywiiza3zEmRB4Tw7eNC5k0VuXVndYOUj9')
GROK_ENDPOINT = os.getenv('GROK_ENDPOINT', 'https://api.x.ai/v1/chat/completions')