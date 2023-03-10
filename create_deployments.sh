#!/bin/bash

name=standard
storage=github/handykapp-etl
infra=docker-container/handykapp-etl

mkdir deployments
poetry run prefect deployment build ./src/extractors/bha_extractor.py:bha_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/bha-deployment.yaml --cron "0 1 * * 3"
poetry run prefect deployment build ./src/extractors/rapid_horseracing_extractor.py:rapid_horseracing_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/rapid-horseracing-deployment.yaml --cron "0 2 * * *"
poetry run prefect deployment build ./src/extractors/theracingapi_extractor.py:theracingapi_racecards_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/theracingapi-deployment.yaml --cron "0 22 * * *"
poetry run prefect deployment build ./src/loaders/load.py:load_database_afresh -n $name -sb $storage -ib $infra -q default -o ./deployments/load-database-afresh-deployment.yaml --cron "0 4 * * *"

poetry run prefect deployment apply ./deployments/bha-deployment.yaml
poetry run prefect deployment apply ./deployments/rapid-horseracing-deployment.yaml
poetry run prefect deployment apply ./deployments/theracingapi-deployment.yaml
poetry run prefect deployment apply ./deployments/load-database-afresh-deployment.yaml
