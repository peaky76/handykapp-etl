from typing import Literal

import pendulum
from horsetalk import Horse
from peak_utility.listish import compact
from prefect import get_run_logger
from pydantic_extra_types.pendulum_dt import Date
from requests import get

from models import MongoHorse, MongoOperation, PreMongoHorse, PreMongoRunner


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


def horse_name_to_pre_mongo_horse(
    name: str,
    *,
    sex: Literal["M", "F"] | None = None,
    sire: PreMongoHorse | None = None,
    default_country: str | None = None,
) -> PreMongoHorse | None:
    if not name:
        return None

    horse = Horse(name)

    if not horse:
        return None

    name = horse.name
    country = horse.country.name if horse.country else None

    params = compact(
        {
            "name": name.upper(),
            "country": country or default_country,
            "sex": sex,
            "sire": sire.model_dump() if sire else None,
        }
    )
    return PreMongoHorse(**params)


def create_gelding_operation(date: Date) -> MongoOperation:
    return MongoOperation(operation_type="gelding", date=date)


def get_operations(horse: PreMongoHorse) -> list[MongoOperation] | None:
    if not horse.gelded_from:
        return None

    return [create_gelding_operation(horse.gelded_from)]


def make_operations_update(
    horse: PreMongoHorse, db_horse: MongoHorse
) -> list[MongoOperation] | None:
    if not hasattr(horse, "gelded_from") or not horse.gelded_from:
        return None

    operations = db_horse.operations

    if not operations:
        return get_operations(horse)

    gelding_op = next((op for op in operations if op.operation_type == "gelding"), None)
    non_gelding_ops = [op for op in operations if op.operation_type != "gelding"]

    if not gelding_op:
        return [*operations, create_gelding_operation(horse.gelded_from)]

    current_date = gelding_op.date

    if current_date is None or horse.gelded_from < current_date:
        return [*non_gelding_ops, create_gelding_operation(horse.gelded_from)]

    return operations
