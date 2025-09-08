import json, os, pathlib, psycopg2, subprocess, sys
from testcontainers.postgres import PostgresContainer

ROOT = pathlib.Path(__file__).resolve().parents[2]

def test_etl_end_to_end(tmp_path):
    with PostgresContainer("postgres:15") as pg:
        url = pg.get_connection_url()
        import urllib.parse as up
        p = up.urlparse(url)
        db, user, pwd, host, port = p.path.lstrip("/"), p.username, p.password, p.hostname, str(p.port)

        # Load schema
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, host=host, port=port)
        with conn, conn.cursor() as cur:
            cur.execute((ROOT/"docs"/"data_model.sql").read_text())

        # Fixture
        sample = [{ "InternalOrderId": 1, "OrderDateUtc": "2025-09-01T05:26:24Z",
                    "LastUpdatedDateUtc": "2025-09-01T05:40:48Z", "OrderStatus": "Paid",
                    "InvoiceStatus":"FullyInvoiced","ShipmentStatus":"FullyShipped",
                    "BillingCustomer": {"InternalCustomerId": 10, "FirstName":"A","LastName":"B","EmailAddress":"a@b.com"},
                    "BillingAddress": {"Id":100,"FirstName":"A","LastName":"B","AddressLine1":"X","City":"C","State":"S","ZipCode":"Z","CountryCode":"DK"},
                    "ShippingAddress": {"Id":101,"FirstName":"A","LastName":"B","AddressLine1":"X","City":"C","State":"S","ZipCode":"Z","CountryCode":"DK"},
                    "SubTotal":100.00,"ShippingTotal":10.00,"DiscountTotal":5.00,"OrderTotal":105.00,"CurrencyCode":"DKK","Channel":"WEB",
                    "LineItems":[{"InternalLineItemId":1000,"SKU":"SKU1","ProductName":"P1","ItemName":"I1","QuantityOrdered":2,"UnitPrice":50.00,"UnitDiscount":0.00,"SubTotal":100.00,"TotalTax":20.00,"Total":100.00,"IsPreOrder":False}],
                    "Taxes":[{"InternalTaxRateId":8,"Amount":20.00,"Rate":0.25,"TaxType":"Net","BackendName":"VAT","PublicTaxName":"VAT 25%"}]
                  }]
        fixture = tmp_path/"orders.json"
        fixture.write_text(json.dumps(sample), encoding="utf-8")

        # Run ETL
        env = os.environ.copy()
        env.update({
            "PGDATABASE": db, "PGUSER": user, "PGPASSWORD": pwd,
            "PGHOST": host, "PGPORT": port, "INPUT_PATH": str(fixture),
            "PYTHONPATH": str(ROOT),
        })
        subprocess.check_call([sys.executable, "-m", "etl.main"], cwd=str(ROOT), env=env)

        # Assertions
        with conn, conn.cursor() as cur:
            for tbl, exp in [("customers",1),("addresses",2),("orders",1),("order_line_items",1),("order_taxes",1)]:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                assert cur.fetchone()[0] == exp