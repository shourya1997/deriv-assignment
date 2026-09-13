.PHONY: up down build test verify logs

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

test:
	docker compose --profile test run --rm tests

verify:
	bash scripts/verify.sh

logs:
	docker compose logs -f
