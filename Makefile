.PHONY: up down build logs ps restart clean test test-unit test-integration bench-1k bench-10k bench-50k fmt

up:
	docker compose up --build -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

restart:
	docker compose restart

clean:
	docker compose down -v --remove-orphans

# Runs the pure-python unit tests (validation, feed health, statistics,
# simulator injection) without requiring Kafka/Postgres to be up.
test-unit:
	pip install -r tests/requirements-dev.txt --break-system-packages
	pytest tests/unit -v

# Requires `make up` to already be running (needs Kafka + Postgres + API).
test-integration:
	pip install -r tests/requirements-dev.txt --break-system-packages
	pytest tests/integration -v

test: test-unit

bench-1k:
	docker compose exec -e SIMULATOR_THROUGHPUT_MSG_PER_SEC=1000 -e SIMULATOR_RUN_DURATION_SECONDS=60 simulator python main.py

bench-10k:
	docker compose exec -e SIMULATOR_THROUGHPUT_MSG_PER_SEC=10000 -e SIMULATOR_RUN_DURATION_SECONDS=60 simulator python main.py

bench-50k:
	docker compose exec -e SIMULATOR_THROUGHPUT_MSG_PER_SEC=50000 -e SIMULATOR_RUN_DURATION_SECONDS=60 simulator python main.py
