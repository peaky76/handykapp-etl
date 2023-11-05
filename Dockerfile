FROM prefecthq/prefect:2.14-python3.11

RUN groupadd -r nonroot
RUN useradd -r -g nonroot nonroot

# Install poetry
RUN pip install --upgrade pip
RUN pip install poetry

# Copy the project to image
COPY /src /opt/handykapp-etl/src
COPY pyproject.toml poetry.lock settings.toml /opt/handykapp-etl/
WORKDIR /opt/handykapp-etl

# Install packages in the system Python
RUN poetry config virtualenvs.create false
RUN poetry config virtualenvs.prefer-active-python true
RUN poetry install --only main

USER nonroot