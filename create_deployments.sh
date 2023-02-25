#!/bin/bash

name=standard
storage=github/handykapp-etl
infra=docker-container/handykapp-etl

mkdir deployments
poetry run prefect deployment build ./flows/bha.py:bha_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/bha-deployment.yaml --cron "0 1 * * 3"
poetry run prefect deployment build ./flows/rapid_horseracing.py:rapid_horseracing_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/rapid-horseracing-deployment.yaml --cron "0 2 * * *"
poetry run prefect deployment build ./flows/load.py:create_fresh_database -n $name -sb $storage -ib $infra -q default -o ./deployments/load.yaml --cron "15 20 * * *"

poetry run prefect deployment apply ./deployments/bha-deployment.yaml
poetry run prefect deployment apply ./deployments/rapid-horseracing-deployment.yaml
poetry run prefect deployment apply ./deployments/load.yaml
