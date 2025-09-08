import json
import os
import psycopg2
from psycopg2.extras import execute_values

DB_CONN = {
  "dbname": "dedb",
    "user": "deuser",
    "password": "secret",
    "host": "127.0.0.1",
    "port": "55432"
}

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def upsert_customers(cur, records):
    rows = []
    for r in records:
        cust = r.get("BillingCustomer")
        if not cust: continue
        rows.append((
            cust["InternalCustomerId"],
            cust.get("ExternalCustomerId"),
            cust.get("UserId"),
            cust.get("FirstName"),
            cust.get("LastName"),
            cust.get("EmailAddress"),
        ))
    sql = """
    INSERT INTO customers (internal_customer_id, external_customer_id, user_id, first_name, last_name, email_address)
    VALUES %s
    ON CONFLICT (internal_customer_id) DO UPDATE
    SET first_name=EXCLUDED.first_name,
        last_name=EXCLUDED.last_name,
        email_address=EXCLUDED.email_address;
    """
    execute_values(cur, sql, rows)

def upsert_orders(cur, records):
    rows = []
    for r in records:
        rows.append((
            r["InternalOrderId"],
            r.get("ExternalOrderId"),
            r["OrderDateUtc"],
            r["LastUpdatedDateUtc"],
            r.get("DeadlineDateUtc"),
            r.get("OrderStatus"),
            r.get("InvoiceStatus"),
            r.get("ShipmentStatus"),
            r["BillingCustomer"]["InternalCustomerId"] if r.get("BillingCustomer") else None,
            None,  # billing_address_id (to be handled later)
            None,  # shipping_address_id (to be handled later)
            r.get("SubTotal"),
            r.get("ShippingTotal"),
            r.get("DiscountTotal"),
            r.get("OrderTotal"),
            r.get("CurrencyCode"),
            r.get("Channel"),
            r.get("Comments"),
            json.dumps(r),
        ))
    sql = """
    INSERT INTO orders (
      internal_order_id, external_order_id, order_date_utc, last_updated_utc, deadline_utc,
      order_status, invoice_status, shipment_status,
      billing_customer_id, billing_address_id, shipping_address_id,
      subtotal, shipping_total, discount_total, order_total,
      currency_code, channel, comments, raw
    )
    VALUES %s
    ON CONFLICT (internal_order_id) DO UPDATE
    SET order_status = EXCLUDED.order_status,
        shipment_status = EXCLUDED.shipment_status,
        last_updated_utc = EXCLUDED.last_updated_utc;
    """
    execute_values(cur, sql, rows)

def main():
    data = load_json("data/orders_data.json")

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            upsert_customers(cur, data)
            upsert_orders(cur, data)
        conn.commit()

if __name__ == "__main__":
    main()