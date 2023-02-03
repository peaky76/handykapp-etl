FROM prefecthq/prefect:2.7.10-python3.10

# Install poetry
RUN pip install --upgrade pip
RUN pip install poetry

# Copy the project to image
COPY /src /opt/handykapp-etl/src
COPY README.md pyproject.toml poetry.lock deploy.sh /opt/handykapp-etl/
WORKDIR /opt/handykapp-etl

# Install packages in the system Python
RUN poetry config virtualenvs.create false
RUN poetry config virtualenvs.prefer-active-python true
RUN poetry install --only main

# ENTRYPOINT ["./deploy.sh"]