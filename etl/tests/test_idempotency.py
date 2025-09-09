import json, os, pathlib, psycopg2, subprocess, sys
from testcontainers.postgres import PostgresContainer

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "docs" / "data_model.sql"

SAMPLE = [{
    "InternalOrderId": 42,
    "OrderDateUtc": "2025-09-01T05:26:24Z",
    "LastUpdatedDateUtc": "2025-09-01T05:40:48Z",
    "OrderStatus": "Paid", "InvoiceStatus":"FullyInvoiced","ShipmentStatus":"FullyShipped",
    "BillingCustomer": {"InternalCustomerId": 4242, "FirstName":"Ada","LastName":"L","EmailAddress":"ada@example.com"},
    "BillingAddress": {"Id":10042,"FirstName":"Ada","LastName":"L","AddressLine1":"X","City":"C","State":"S","ZipCode":"Z","CountryCode":"DK"},
    "ShippingAddress": {"Id":10043,"FirstName":"Ada","LastName":"L","AddressLine1":"X","City":"C","State":"S","ZipCode":"Z","CountryCode":"DK"},
    "SubTotal":100.00,"ShippingTotal":10.00,"DiscountTotal":5.00,"OrderTotal":105.00,"CurrencyCode":"DKK","Channel":"WEB",
    "LineItems":[{"InternalLineItemId":42001,"SKU":"SKU1","ProductName":"P1","ItemName":"I1","QuantityOrdered":2,"UnitPrice":50.00,"UnitDiscount":0.00,"SubTotal":100.00,"TotalTax":20.00,"Total":100.00,"IsPreOrder":False}],
    "Taxes":[{"InternalTaxRateId":8,"Amount":20.00,"Rate":0.25,"TaxType":"Net","BackendName":"VAT","PublicTaxName":"VAT 25%"}]
}]

def run_etl(env, cwd):
    subprocess.check_call([sys.executable, "-m", "etl.main"], cwd=str(cwd), env=env)

def test_idempotent_re_runs(tmp_path):
    with PostgresContainer("postgres:15") as pg:
        # Parse container URL
        import urllib.parse as up
        p = up.urlparse(pg.get_connection_url())
        db, user, pwd, host, port = p.path.lstrip("/"), p.username, p.password, p.hostname, str(p.port)

        # Load schema
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, host=host, port=port)
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

        # Fixture
        fx = tmp_path/"orders.json"
        fx.write_text(json.dumps(SAMPLE), encoding="utf-8")

        env = os.environ.copy()
        env.update({"PGDATABASE":db, "PGUSER":user, "PGPASSWORD":pwd,
                    "PGHOST":host, "PGPORT":port, "INPUT_PATH":str(fx),
                    "PYTHONPATH":str(REPO_ROOT)})

        # First run
        run_etl(env, REPO_ROOT)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders"); c1_orders = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM order_line_items"); c1_lines = cur.fetchone()[0]

        # Second run (should be identical counts)
        run_etl(env, REPO_ROOT)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders"); c2_orders = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM order_line_items"); c2_lines = cur.fetchone()[0]

        assert (c1_orders, c1_lines) == (c2_orders, c2_lines), "Counts changed on re-run (not idempotent)"

        # Update the shipment status and verify the same PK got updated
        updated = SAMPLE.copy()
        updated[0] = dict(SAMPLE[0], ShipmentStatus="PartiallyShipped")
        fx.write_text(json.dumps(updated), encoding="utf-8")
        run_etl(env, REPO_ROOT)

        with conn, conn.cursor() as cur:
            cur.execute("SELECT shipment_status FROM orders WHERE internal_order_id=42")
            (status,) = cur.fetchone()
            assert status == "PartiallyShipped", "Row not updated on UPSERT"