from dotenv import load_dotenv
from supabase import create_client

import os 


load_dotenv(override=True)


SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_KEY)
