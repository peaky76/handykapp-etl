#!/bin/bash

image_name="prefect-poetry-peaky"
tag="latest"
docker_hub_username="peaky76"

# Step 1: Build a new version of the Docker image locally
docker build -t "${image_name}:${tag}" .

# Step 2: Tag the new version of the image with the Docker Hub repository name
docker tag "${image_name}:${tag}" "${docker_hub_username}/${image_name}:${tag}"

# Step 3: Push the new version of the image to Docker Hub
docker push "${docker_hub_username}/${image_name}:${tag}"
