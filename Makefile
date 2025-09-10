.PHONY: up reset etl test down logs

# Build images (if needed) and start ONLY the DB
up:
	docker compose up -d --build db

# Wipe volumes and start a clean DB
reset:
	docker compose down -v && docker compose up -d db

# Run the ETL once in a container (no local venv needed)
etl:
	docker compose run --rm etl

# Run tests locally (they use Testcontainers for DB)
test:
	pytest -q

# Stop all services (keep volumes)
down:
	docker compose down -v

# Tail DB logs
logs:
	docker compose logs -f db