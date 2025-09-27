FROM prefecthq/prefect:3.4.20-python3.13

RUN groupadd -r nonroot
RUN useradd -r -g nonroot nonroot

# Install poetry
RUN pip install --upgrade pip
RUN pip install poetry

# Copy the project to image
COPY /src /opt/handykapp-etl/src
COPY pyproject.toml poetry.lock settings.toml /opt/handykapp-etl/
WORKDIR /opt/handykapp-etl

# Fix permissions so nonroot user can write to the working directory
RUN chmod -R 777 /opt/handykapp-etl

# Install packages in the system Python
RUN poetry config virtualenvs.create false
RUN poetry install --only main --no-root

USER nonroot