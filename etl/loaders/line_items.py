from psycopg2.extras import execute_values

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
    return len(rows)