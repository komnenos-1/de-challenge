import psycopg2
from contextlib import contextmanager
from .config import DB_CONN

@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONN)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()