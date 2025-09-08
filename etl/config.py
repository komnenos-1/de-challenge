import os

DB_CONN = {
    "dbname": os.getenv("PGDATABASE", "dedb"),
    "user": os.getenv("PGUSER", "deuser"),
    "password": os.getenv("PGPASSWORD", "secret"),
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": os.getenv("PGPORT", "55432"),
}

INPUT_PATH = os.getenv("INPUT_PATH", "data/orders_data.json")