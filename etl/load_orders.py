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

# ---------------------customers --------------------------
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

# ---------------------addresses --------------------------
def _addr_tuple(a):
    return (
        a["Id"],
        a.get("ExternalAddressId"),
        a.get("FirstName"),
        a.get("LastName"),
        a.get("AddressLine1"),
        a.get("City"),
        a.get("State"),
        a.get("ZipCode"),
        a.get("CountryCode"),
    )

def upsert_addresses(cur, records):
    rows, seen = [], set()
    for r in records:
        for key in ("BillingAddress", "ShippingAddress"):
            a = r.get(key) or {}
            aid = a.get("Id")
            if aid is None or aid in seen:
                continue
            seen.add(aid)
            rows.append(_addr_tuple(a))
    if not rows:
        return
    sql = """
    INSERT INTO addresses (address_id, external_address_id, first_name, last_name, address_line1, city, state, postal_code, country_code)
    VALUES %s
    ON CONFLICT (address_id) DO UPDATE
    SET external_address_id = EXCLUDED.external_address_id,
        first_name          = EXCLUDED.first_name,
        last_name           = EXCLUDED.last_name,
        address_line1       = EXCLUDED.address_line1,
        city                = EXCLUDED.city,
        state               = EXCLUDED.state,
        postal_code         = EXCLUDED.postal_code,
        country_code        = EXCLUDED.country_code;
    """
    execute_values(cur, sql, rows)

# ---------------------orders --------------------------
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

# ---------------------line items --------------------------
def upsert_order_line_items(cur, records):
    rows = []
    for r in records:
        oid = r["InternalOrderId"]
        for li in (r.get("LineItems") or []):
            rows.append((
                li["InternalLineItemId"],
                oid,
                li.get("SKU"),
                li.get("ProductName"),
                li.get("ItemName"),
                li.get("Description"),
                li.get("QuantityOrdered"),
                li.get("QuantityInvoiced"),
                li.get("QuantityShipped"),
                li.get("QuantityCancelled"),
                li.get("QuantityReturned"),
                li.get("UnitPrice"),
                li.get("UnitDiscount"),
                li.get("SubTotal"),
                li.get("TotalTax"),
                li.get("Total"),
                li.get("IsPreOrder"),
            ))
    if not rows:
        return
    sql = """
    INSERT INTO order_line_items (
      internal_line_item_id, internal_order_id, sku, product_name, item_name, description,
      quantity_ordered, quantity_invoiced, quantity_shipped, quantity_cancelled, quantity_returned,
      unit_price, unit_discount, subtotal, total_tax, total, is_preorder
    )
    VALUES %s
    ON CONFLICT (internal_line_item_id) DO UPDATE
    SET internal_order_id = EXCLUDED.internal_order_id,
        sku               = EXCLUDED.sku,
        product_name      = EXCLUDED.product_name,
        item_name         = EXCLUDED.item_name,
        description       = EXCLUDED.description,
        quantity_ordered  = EXCLUDED.quantity_ordered,
        quantity_invoiced = EXCLUDED.quantity_invoiced,
        quantity_shipped  = EXCLUDED.quantity_shipped,
        quantity_cancelled= EXCLUDED.quantity_cancelled,
        quantity_returned = EXCLUDED.quantity_returned,
        unit_price        = EXCLUDED.unit_price,
        unit_discount     = EXCLUDED.unit_discount,
        subtotal          = EXCLUDED.subtotal,
        total_tax         = EXCLUDED.total_tax,
        total             = EXCLUDED.total,
        is_preorder       = EXCLUDED.is_preorder;
    """
    execute_values(cur, sql, rows)

# ---------------------order level tax --------------------------
def upsert_order_taxes(cur, records):
    rows = []
    for r in records:
        oid = r["InternalOrderId"]
        for t in (r.get("Taxes") or []):
            rows.append((
                oid,
                t["InternalTaxRateId"],
                t.get("Amount"),
                t.get("Rate"),
                t.get("TaxType"),
                t.get("BackendName"),
                t.get("PublicTaxName"),
            ))
    if not rows:
        return
    sql = """
    INSERT INTO order_taxes (
      internal_order_id, internal_tax_rate_id, tax_amount, tax_rate, tax_type, backend_name, public_tax_name
    )
    VALUES %s
    ON CONFLICT (internal_order_id, internal_tax_rate_id) DO UPDATE
    SET tax_amount   = EXCLUDED.tax_amount,
        tax_rate     = EXCLUDED.tax_rate,
        tax_type     = EXCLUDED.tax_type,
        backend_name = EXCLUDED.backend_name,
        public_tax_name = EXCLUDED.public_tax_name;
    """
    execute_values(cur, sql, rows)

def main():
    data = load_json("data/orders_data.json")

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            upsert_customers(cur, data)
            upsert_orders(cur, data)
            upsert_addresses(cur, data)
            upsert_order_line_items(cur, data)
            upsert_order_taxes(cur, data)
        conn.commit()

if __name__ == "__main__":
    main()