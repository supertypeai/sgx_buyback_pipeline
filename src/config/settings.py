from dotenv import load_dotenv
from supabase import create_client

import os 
import logging

load_dotenv(override=True)

logging.basicConfig(
    filename='scraper.log', 
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
