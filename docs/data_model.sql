CREATE TABLE IF NOT EXISTS customers (
  internal_customer_id BIGINT PRIMARY KEY,
  external_customer_id TEXT,
  user_id              TEXT,
  first_name           TEXT,
  last_name            TEXT,
  email_address        TEXT
);

CREATE TABLE IF NOT EXISTS addresses (
  address_id           BIGINT PRIMARY KEY,
  external_address_id  TEXT,
  first_name           TEXT,
  last_name            TEXT,
  address_line1        TEXT,
  city                 TEXT,
  state                TEXT,
  postal_code          TEXT,
  country_code         TEXT
);

-- ─────────── Orders ───────────
CREATE TABLE IF NOT EXISTS orders (
  internal_order_id    BIGINT PRIMARY KEY,
  external_order_id    TEXT,
  order_date_utc       TIMESTAMPTZ NOT NULL,
  last_updated_utc     TIMESTAMPTZ NOT NULL,
  deadline_utc         TIMESTAMPTZ,
  order_status         TEXT,
  invoice_status       TEXT,
  shipment_status      TEXT,
  billing_customer_id  BIGINT REFERENCES customers(internal_customer_id),
  billing_address_id   BIGINT REFERENCES addresses(address_id),
  shipping_address_id  BIGINT REFERENCES addresses(address_id),
  subtotal             NUMERIC(12,2),
  shipping_total       NUMERIC(12,2),
  discount_total       NUMERIC(12,2),
  order_total          NUMERIC(12,2),
  currency_code        TEXT,
  channel              TEXT,
  comments             TEXT,
  raw                  JSONB
);

CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date_utc);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status, shipment_status);

-- ─────────── Order line items ───────────
CREATE TABLE IF NOT EXISTS order_line_items (
  internal_line_item_id BIGINT PRIMARY KEY,
  internal_order_id     BIGINT NOT NULL REFERENCES orders(internal_order_id) ON DELETE CASCADE,
  sku                   TEXT,
  product_name          TEXT,
  item_name             TEXT,
  description           TEXT,
  quantity_ordered      NUMERIC(12,3),
  quantity_invoiced     NUMERIC(12,3),
  quantity_shipped      NUMERIC(12,3),
  quantity_cancelled    NUMERIC(12,3),
  quantity_returned     NUMERIC(12,3),
  unit_price            NUMERIC(12,2),
  unit_discount         NUMERIC(12,2),
  subtotal              NUMERIC(12,2),
  total_tax             NUMERIC(12,2),
  total                 NUMERIC(12,2),
  is_preorder           BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_line_items_order ON order_line_items(internal_order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_sku ON order_line_items(sku);

-- ─────────── Order-level taxes ───────────
CREATE TABLE IF NOT EXISTS order_taxes (
  internal_order_id     BIGINT NOT NULL REFERENCES orders(internal_order_id) ON DELETE CASCADE,
  internal_tax_rate_id  BIGINT NOT NULL,
  tax_amount            NUMERIC(12,2),
  tax_rate              NUMERIC(7,4),
  tax_type              TEXT,
  backend_name          TEXT,
  public_tax_name       TEXT,
  PRIMARY KEY (internal_order_id, internal_tax_rate_id)
);