import os

DB_CONN = {
  "dbname": "dedb",
    "user": "deuser",
    "password": "secret",
    "host": "127.0.0.1",
    "port": "55432"
}

INPUT_PATH = os.getenv("INPUT_PATH", "data/orders_data.json")