PROJECT_NAME := $(if $(PROJECT_NAME),$(PROJECT_NAME),maestro-python-client)

.PHONY: all up bare_test down test run

up:
	docker compose -p ${PROJECT_NAME} up -d

bare_test:
	$(eval REDIS_PORT:=$(shell ./scripting/docker-compose-get-port.sh ${PROJECT_NAME} redis 6379/tcp))
	@echo "Using redis port ${REDIS_PORT}"
	REDIS_PORT=${REDIS_PORT} pipenv run python -m pytest $(ARGS)

down:
	-docker compose -p ${PROJECT_NAME} down

test: down up bare_test down

lint:
	pipenv run pre-commit run -a

all: test run
