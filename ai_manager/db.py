import os
import psycopg2
from contextlib import contextmanager

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv is optional in prod (env vars may already be set)
    pass

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

@contextmanager
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()
