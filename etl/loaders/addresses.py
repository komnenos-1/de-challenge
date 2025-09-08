from psycopg2.extras import execute_values

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
    return len(rows)