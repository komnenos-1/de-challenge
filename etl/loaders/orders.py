import json
from psycopg2.extras import execute_values

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
    return len(rows)