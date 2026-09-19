.PHONY: help up down topics seed train test lint logs reset loadtest

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

up:        ## Build and start the whole platform
	docker compose up -d --build
	@echo "waiting for kafka ..." && sleep 25
	$(MAKE) topics

down:      ## Stop everything
	docker compose down

reset:     ## Stop and wipe all volumes
	docker compose down -v

topics:    ## Create Kafka topics
	docker compose exec kafka bash -c "KAFKA_BOOTSTRAP=kafka:29092 $$(cat scripts/create_topics.sh)"

seed:      ## Load customer profiles and a demo fraud ring
	python scripts/seed.py

train:     ## Train the anomaly model
	python ml/train.py --rows 200000 --out ml/model.joblib

test:      ## Run unit tests
	pytest -q

lint:      ## Lint and type check
	ruff check libs services ml scripts tests && mypy libs --ignore-missing-imports

logs:      ## Tail all service logs
	docker compose logs -f --tail=50 risk-scoring rule-engine ml-service graph-service

loadtest:  ## Fire 200 rps for 60 seconds
	python scripts/loadtest.py --rps 200 --seconds 60
