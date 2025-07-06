import os
from sqlalchemy import create_engine

def get_engine():
    DATABASE_URL = os.getenv('DATABASE_URL')
    return create_engine(DATABASE_URL)
