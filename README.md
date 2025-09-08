# Data Engineer Challenge – ETL Pipeline

This project demonstrates a simple ETL pipeline:
- Ingests **orders JSON**.
- Transforms into a normalized relational schema (customers, addresses, orders, line items, taxes).
- Loads into **Postgres**.
- Ensures **idempotency** with `ON CONFLICT DO UPDATE`.
- Tested end-to-end using **pytest + testcontainers**.

---

## Quickstart

### 1. Start Postgres

docker compose up -d

### 2. Run ETL

python -m etl.main

### 3. Inspect data

Connect with DBeaver or psql:
docker exec -it de_db psql -U deuser -d dedb -c "SELECT COUNT(*) FROM orders;"

### 4. Reset the database

docker compose down -v && docker compose up -d

### 5. Run tests

pytest -q

## Example queries

### Revenue per day

SELECT order_date_utc::date AS day, SUM(order_total) AS revenue
FROM orders
GROUP BY 1
ORDER BY 1;

### Top SKUs

SELECT sku, SUM(quantity_ordered) AS qty
FROM order_line_items
GROUP BY sku
ORDER BY qty DESC
LIMIT 5;

## Project structure

etl/
  ├── main.py          # Entry point
  ├── db.py            # Connection utils
  ├── loaders/         # Loader functions
  ├── tests/           # Integration tests
docs/
  ├── data_model.sql   # Schema DDL
  ├── diagram.png      # ERD (optional export)
data/
  ├── orders_data.json # Sample input
docker-compose.yml

