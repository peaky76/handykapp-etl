#!/bin/bash

name=standard
storage=github/handykapp-etl
infra=docker-container/handykapp-etl

mkdir deployments
poetry run prefect deployment build ./flows/bha.py:bha_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/bha-deployment.yaml --cron "0 23 * * 2"
poetry run prefect deployment build ./flows/rapid_horseracing.py:rapid_horseracing_extractor -n $name -sb $storage -ib $infra -q default -o ./deployments/rapid-horseracing-deployment.yaml --cron "0 2 * * *"
poetry run prefect deployment apply ./deployments/bha-deployment.yaml
poetry run prefect deployment apply ./deployments/rapid-horseracing-deployment.yaml