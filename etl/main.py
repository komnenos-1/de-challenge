import json
import os
import psycopg2
from psycopg2.extras import execute_values
from etl.db import get_conn
from etl.config import INPUT_PATH
from etl.loaders.customers import upsert_customers
from etl.loaders.addresses import upsert_addresses
from etl.loaders.orders import upsert_orders
from etl.loaders.line_items import upsert_order_line_items
from etl.loaders.order_taxes import upsert_order_taxes


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data if isinstance(data, list) else [data]

def run():
    records = load_json(INPUT_PATH)
    with get_conn() as conn:
        with conn.cursor() as cur:
            n1 = upsert_customers(cur, records)
            n2 = upsert_addresses(cur, records)
            n3 = upsert_orders(cur, records)
            n4 = upsert_order_line_items(cur, records)
            n5 = upsert_order_taxes(cur, records)
            print(f"customers={n1}, addresses={n2}, orders={n3}, line_items={n4}, order_taxes={n5}")

if __name__ == "__main__":
    run()