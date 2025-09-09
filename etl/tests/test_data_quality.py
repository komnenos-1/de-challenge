import json
import os
import pathlib
import psycopg2
import subprocess
import sys
from typing import Dict, Any, Tuple
from testcontainers.postgres import PostgresContainer
from pathlib import Path
import urllib.parse as up
import pytest

# -------- helpers to resolve repo root & schema path --------
def find_upwards(start: Path, rel: str) -> Path:
    cur = start.resolve()
    for _ in range(6):
        cand = cur / rel
        if cand.exists():
            return cand
        cur = cur.parent
    raise FileNotFoundError(f"Could not find {rel} starting from {start}")

THIS_FILE = Path(__file__)
SCHEMA_PATH = find_upwards(THIS_FILE, "docs/data_model.sql")
REPO_ROOT = SCHEMA_PATH.parent.parent  # repo root (parent of docs/)

# Sample data for validations
SAMPLE = [{
    "InternalOrderId": 777,
    "OrderDateUtc": "2025-09-01T05:26:24Z",
    "LastUpdatedDateUtc": "2025-09-01T05:40:48Z",
    "OrderStatus": "Paid",
    "InvoiceStatus": "FullyInvoiced",
    "ShipmentStatus": "FullyShipped",
    "BillingCustomer": {
        "InternalCustomerId": 12345,
        "FirstName": "Dana",
        "LastName": "S",
        "EmailAddress": "dana@example.com"
    },
    "BillingAddress": {
        "Id": 91011, "FirstName": "Jane", "LastName": "Doe",
        "AddressLine1": "Vesterbrogade 1", "City": "Copenhagen",
        "State": "H", "ZipCode": "2100", "CountryCode": "DK"
    },
    "ShippingAddress": {
        "Id": 91012, "FirstName": "Jane", "LastName": "Doe",
        "AddressLine1": "Vesterbrogade", "City": "Copenhagen",
        "State": "H", "ZipCode": "2100", "CountryCode": "DK"
    },
    "SubTotal": 100.00,
    "ShippingTotal": 10.00,
    "DiscountTotal": 5.00,
    "OrderTotal": 105.00,
    "CurrencyCode": "DKK",
    "Channel": "WEB",
    "LineItems": [
        {
            "InternalLineItemId": 777001,
            "SKU": "SKU-777",
            "ProductName": "Widget",
            "ItemName": "Widget",
            "QuantityOrdered": 2,
            "UnitPrice": 50.00,
            "UnitDiscount": 0.00,
            "SubTotal": 100.00,
            "TotalTax": 20.00,
            "Total": 100.00,
            "IsPreOrder": False
        }
    ],
    "Taxes": [
        {
            "InternalTaxRateId": 8,
            "Amount": 20.00,
            "Rate": 0.25,
            "TaxType": "Net",
            "BackendName": "VAT DK 0.25",
            "PublicTaxName": "VAT DK 25%"
        }
    ]
}]

# -------- pytest fixtures: spin up DB once per module, run ETL once --------
@pytest.fixture(scope="module")
def pg_env(tmp_path_factory):
    """Start disposable Postgres, load schema, run ETL on a small fixture.
    Yields (conn, env) where conn is a psycopg2 connection and env are the ETL env vars used."""
    # Start Postgres container
    with PostgresContainer("postgres:15") as pg:
        url = pg.get_connection_url()  # postgresql://user:pwd@host:port/db
        p = up.urlparse(url)
        db, user, pwd, host, port = (
            p.path.lstrip("/"), p.username, p.password, p.hostname, str(p.port)
        )

        # Connect and load schema
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, host=host, port=port)
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

        # Write fixture
        tmp_path = tmp_path_factory.mktemp("dq")
        fixture = tmp_path / "orders.json"
        fixture.write_text(json.dumps(SAMPLE), encoding="utf-8")

        # Prepare env for ETL subprocess
        env = os.environ.copy()
        env.update({
            "PGDATABASE": db,
            "PGUSER": user,
            "PGPASSWORD": pwd,
            "PGHOST": host,
            "PGPORT": port,
            "INPUT_PATH": str(fixture),
            "PYTHONPATH": str(REPO_ROOT),  # ensure 'etl' is importable
        })

        # Run ETL using the same interpreter running pytest
        subprocess.check_call([sys.executable, "-m", "etl.main"], cwd=str(REPO_ROOT), env=env)

        # Yield to tests
        try:
            yield conn, env
        finally:
            conn.close()

# -------- Tests --------
def test_orders_have_ids(pg_env):
    conn, _ = pg_env
    with conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders WHERE internal_order_id IS NULL;")
        (bad,) = cur.fetchone()
        assert bad == 0, "Found orders with NULL internal_order_id"

def test_amounts_non_negative(pg_env):
    conn, _ = pg_env
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM orders
            WHERE subtotal < 0
               OR shipping_total < 0
               OR discount_total < 0
               OR order_total < 0;
        """)
        (bad,) = cur.fetchone()
        assert bad == 0, "Found orders with negative monetary amounts"

def test_order_reconciliation(pg_env):
    """Check order_total ≈ subtotal - discount_total + shipping_total (±0.02). 
    Taxes appear to be VAT-inclusive in line prices; tax rows are informational."""
    conn, _ = pg_env
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM orders
            WHERE ABS((subtotal - discount_total + shipping_total) - order_total) > 0.02;
        """)
        (bad,) = cur.fetchone()
        assert bad == 0, "Found orders where subtotal - discount + shipping != order_total"