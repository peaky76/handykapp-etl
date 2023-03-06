# To allow running as a script
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import log_validation_problem, read_file, stream_file
from prefect import flow, task
import petl
import yaml
import pendulum


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["rapid_horseracing"]["spaces"]["dir"]


def validate_date(date):
    try:
        pendulum.parse(date)
        return True
    except pendulum.exceptions.ParserError:
        return False
    except TypeError:
        return False


@task(tags=["Rapid"])
def read_json(json):
    source = petl.MemorySource(stream_file(json))
    return petl.fromjson(source)


@task(tags=["Rapid"])
def validate_result(data):
    header = (
        "id_race",
        "course",
        "date",
        "title",
        "distance",
        "age",
        "going",
        "finished",
        "canceled",
        "finish_time",
        "prize",
        "class",
        "horses",
    )
    constraints = [
        dict(name="course_str", field="course", test=str),
        dict(name="date_valid", field="date", assertion=validate_date),
        dict(name="title_str", field="title", test=str),
        dict(name="distance_valid", field="distance", assertion=lambda x: x),
        dict(name="age_int", field="age", test=int),
        dict(name="going_valid", field="going", assertion=lambda x: x),
        dict(name="finished_bool", field="finished", test=bool),
        dict(name="canceled_bool", field="canceled", test=bool),
        dict(name="prize_valid", field="prize", assertion=lambda x: x),
        dict(name="class_int", field="class", test=int),
        dict(name="horses_list", field="horses", test=list),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def rapid_horseracing_transformer():
    data = read_file(f"{SOURCE}results/rapid_api_result_187431.json")
    problems = validate_result(petl.fromdicts([data]))
    for problem in problems.dicts():
        log_validation_problem(problem)
    return data


if __name__ == "__main__":
    data = rapid_horseracing_transformer()
    print(data)
