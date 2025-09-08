from psycopg2.extras import execute_values

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
    return len(rows)