#!/bin/sh

# echo 'Logging into prefect cloud...'
prefect cloud login -k <API_KEY> -w <API_WORKSPACE>

echo 'Deploying flows...'
cd src
prefect deployment apply bha_extractor-deployment.yaml
prefect deployment apply rapid_horseracing_extractor-deployment.yaml
cd ..

exec /opt/prefect/entrypoint.sh "$@"
