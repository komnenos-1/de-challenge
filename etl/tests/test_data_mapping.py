import json, os, pathlib, psycopg2, subprocess, sys
from pathlib import Path
from testcontainers.postgres import PostgresContainer
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

THIS = Path(__file__)
SCHEMA_PATH = find_upwards(THIS, "docs/data_model.sql")
REPO_ROOT = SCHEMA_PATH.parent.parent  # repo root

# -------- a fixture we can assert against --------
SAMPLE = [{
    "InternalOrderId": 555,
    "ExternalOrderId": "EXT-555",
    "OrderDateUtc": "2025-09-01T05:26:24Z",
    "LastUpdatedDateUtc": "2025-09-01T05:40:48Z",
    "OrderStatus": "Paid",
    "InvoiceStatus": "FullyInvoiced",
    "ShipmentStatus": "FullyShipped",
    "BillingCustomer": {
        "InternalCustomerId": 9999,
        "FirstName": "Alice",
        "LastName": "Ng",
        "EmailAddress": "alice@example.com"
    },
    "BillingAddress": {
        "Id": 2001, "FirstName": "Alice", "LastName": "Ng",
        "AddressLine1": "Nybrogade 2", "City": "Copenhagen",
        "State": "H", "ZipCode": "1203", "CountryCode": "DK"
    },
    "ShippingAddress": {
        "Id": 2002, "FirstName": "Alice", "LastName": "Ng",
        "AddressLine1": "Nybrogade 2", "City": "Copenhagen",
        "State": "COPENHAGEN", "ZipCode": "1203", "CountryCode": "DK"
    },
    "SubTotal": 100.00, "ShippingTotal": 10.00, "DiscountTotal": 5.00, "OrderTotal": 105.00,
    "CurrencyCode": "DKK", "Channel": "WEB",
    "LineItems": [
        {
            "InternalLineItemId": 555001,
            "SKU": "SKU-RED-42",
            "ProductName": "Redacted 42",
            "ItemName": "Special Widget",
            "Description": "A demo widget",
            "QuantityOrdered": 2, "QuantityInvoiced": 2, "QuantityShipped": 2,
            "QuantityCancelled": 0, "QuantityReturned": 0,
            "UnitPrice": 50.00, "UnitDiscount": 0.00,
            "SubTotal": 100.00, "TotalTax": 20.00, "Total": 100.00,
            "IsPreOrder": False
        }
    ],
    "Taxes": [
        {"InternalTaxRateId": 8, "Amount": 20.00, "Rate": 0.25, "TaxType": "Net",
         "BackendName": "VAT DK 0.25", "PublicTaxName": "VAT DK 25%"}
    ]
}]

@pytest.fixture(scope="module")
def loaded_db(tmp_path_factory):
    """Start a temporary Postgres, load schema, run ETL on our SAMPLE once. Yield a psycopg2 connection."""
    with PostgresContainer("postgres:15") as pg:
        url = pg.get_connection_url()
        p = up.urlparse(url)
        db, user, pwd, host, port = p.path.lstrip("/"), p.username, p.password, p.hostname, str(p.port)

        # Load schema
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, host=host, port=port)
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

        # Write fixture file
        tmp = tmp_path_factory.mktemp("mapping")
        fixture = tmp / "orders.json"
        fixture.write_text(json.dumps(SAMPLE), encoding="utf-8")

        # Run ETL as a subprocess (so it uses your package)
        env = os.environ.copy()
        env.update({
            "PGDATABASE": db, "PGUSER": user, "PGPASSWORD": pwd,
            "PGHOST": host, "PGPORT": port, "INPUT_PATH": str(fixture),
            "PYTHONPATH": str(REPO_ROOT),
        })
        subprocess.check_call([sys.executable, "-m", "etl.main"], cwd=str(REPO_ROOT), env=env)

        try:
            yield conn
        finally:
            conn.close()

def test_order_row_matches_source(loaded_db):
    conn = loaded_db
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT internal_order_id, external_order_id, order_status, shipment_status, currency_code, channel
            FROM orders WHERE internal_order_id = %s
        """, (555,))
        row = cur.fetchone()
        assert row is not None, "Order 555 not loaded"
        internal_order_id, external_order_id, order_status, shipment_status, currency, channel = row
        assert internal_order_id == 555
        assert external_order_id == "EXT-555"
        assert order_status == "Paid"
        assert shipment_status == "FullyShipped"
        assert currency == "DKK"
        assert channel == "WEB"

def test_customer_row_matches_source(loaded_db):
    conn = loaded_db
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT internal_customer_id, first_name, last_name, email_address
            FROM customers WHERE internal_customer_id = %s
        """, (9999,))
        row = cur.fetchone()
        assert row is not None, "Customer 9999 not loaded"
        cid, first, last, email = row
        assert cid == 9999
        assert first == "Alice"
        assert last == "Ng"
        assert email == "alice@example.com"

def test_address_row_matches_source(loaded_db):
    conn = loaded_db
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT address_id, city, state, postal_code, country_code
            FROM addresses WHERE address_id = %s
        """, (2001,))
        row = cur.fetchone()
        assert row is not None, "Address 2001 not loaded"
        aid, city, state, postal, country = row
        assert aid == 2001
        assert city == "Copenhagen"
        assert state in ("H", "DEFAULT", "COPENHAGEN")  # tolerate data variations
        assert postal == "1203"
        assert country == "DK"

def test_line_item_row_matches_source(loaded_db):
    conn = loaded_db
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT internal_line_item_id, sku, product_name, item_name, quantity_ordered, unit_price
            FROM order_line_items WHERE internal_line_item_id = %s
        """, (555001,))
        row = cur.fetchone()
        assert row is not None, "Line item 555001 not loaded"
        li_id, sku, product_name, item_name, qty, unit_price = row
        assert li_id == 555001
        assert sku == "SKU-RED-42"
        assert product_name == "Redacted 42"
        assert item_name == "Special Widget"
        assert qty == 2
        assert float(unit_price) == 50.00