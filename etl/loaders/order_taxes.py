from psycopg2.extras import execute_values

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
    return len(rows)