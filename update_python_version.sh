#!/bin/bash

# Updates .python_version file from pyproject.toml

# Read the version constraint from the pyproject.toml file
version=$(grep 'python =' pyproject.toml | awk -F'"' '{print $2}')

# Update the .python_version file with the version
echo "[version]" > .python_version
echo "python = $version" >> .python_version
