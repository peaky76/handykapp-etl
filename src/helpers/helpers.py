from typing import Literal

import pendulum
from horsetalk import Horse
from peak_utility.listish import compact
from prefect import get_run_logger
from requests import get

from models import PreMongoHorse


def fetch_content(url, params=None, headers=None):
    response = get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.content


def get_last_occurrence_of(weekday):
    return pendulum.now().add(days=1).previous(weekday).date()


def log_validation_problem(problem):
    msg = f"{problem['error']} in row {problem['row']} for {problem['field']}: {problem['value']}"
    logger = get_run_logger()
    logger.warning(msg)


type NewmarketRacecourse = Literal["Newmarket July", "Newmarket Rowley"]


def apply_newmarket_workaround(date: pendulum.DateTime) -> NewmarketRacecourse:
    return "Newmarket July" if date.month in (6, 7, 8) else "Newmarket Rowley"


def horse_name_to_pre_mongo_horse(name: str) -> PreMongoHorse:
    horse = Horse(name)
    params = compact(
        {
            "name": horse.name,
            "country": horse.country,
        }
    )
    return PreMongoHorse(params)
