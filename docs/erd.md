# Data Dictionary (MVP)

## customers
**Purpose:** Buyer identity (billing customer) for order joins.
- `internal_customer_id` (PK) — Source natural key.
- `external_customer_id` — Optional external id.
- `user_id` — Platform user id.
- `first_name`, `last_name`, `email_address` — Contact info (nullable).

## addresses
**Purpose:** Reusable billing/shipping address records.
- `address_id` (PK) — Source key.
- `external_address_id` — Optional.
- `first_name`, `last_name`, `address_line1`, `city`, `state`, `postal_code`, `country_code`.

## orders
**Purpose:** Order-level fact (status, totals, dates, relationships).
- `internal_order_id` (PK), `external_order_id`.
- `order_date_utc`, `last_updated_utc`, `deadline_utc`.
- `order_status`, `invoice_status`, `shipment_status`.
- `billing_customer_id` → `customers`, `billing_address_id`/`shipping_address_id` → `addresses`.
- `subtotal`, `shipping_total`, `discount_total`, `order_total`, `currency_code`, `channel`, `comments`.
- `raw` — Original order JSON for audit/edge fields.

## order_line_items
**Purpose:** Line-level fact (what was sold).
- `internal_line_item_id` (PK), `internal_order_id` → `orders`.
- `sku`, `product_name`, `item_name`, `description`.
- Quantities: `quantity_ordered`, `quantity_invoiced`, `quantity_shipped`, `quantity_cancelled`, `quantity_returned`.
- Prices: `unit_price`, `unit_discount`, `subtotal`, `total_tax`, `total`.
- Flags: `is_preorder`.

## order_taxes
**Purpose:** Order-level tax breakdown.
- PK: (`internal_order_id`, `internal_tax_rate_id`) → `orders`.
- `tax_amount`, `tax_rate`, `tax_type`, `backend_name`, `public_tax_name`.