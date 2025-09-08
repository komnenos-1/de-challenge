up:
	docker compose up -d

reset:
	docker compose down -v && docker compose up -d

run-etl:
	python3 -m etl.main

test:
	pytest -q