from dotenv import load_dotenv
from supabase import create_client

import os 
import logging

load_dotenv(override=True)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler('scraper.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)

LOGGER.addHandler(file_handler)

LOGGER.info("Init Global Variable")

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
PROXY = os.getenv('proxy')

SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
