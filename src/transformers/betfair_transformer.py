import re
import sys
from pathlib import Path
from typing import List

import pendulum
import petl

sys.path.append(str(Path(__file__).resolve().parent.parent))

import tomllib
from helpers import get_files, log_validation_problem, stream_file
from models.mongo_betfair_horserace_pnl import MongoBetfairHorseracePnl
from prefect import flow, task

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["betfair"]["spaces_dir"]

@task(tags=["BHA"], task_run_name="get_{date}_{csv_type}_csv")
def get_csv(csv_type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(get_files(SOURCE))
        if csv_type in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None

@task(tags=["Betfair"])
def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source, encoding="utf-8")


@task(tags=["Betfair"])
def transform_betfair_pnl_data(data: petl.Table) -> List[MongoBetfairHorseracePnl]:
    transformed_data = (
        petl.rename(data, {
            "\ufeffMarket": "market",
            "Start time": "race_datetime",
            "Settled date": "settled_date",
            "Profit/Loss (£)": "profit_loss"
        })
        .convert("profit_loss", float)
        .addfield("sport", lambda rec: rec["market"].split("/")[0].strip())
        .addfield("market_detail", lambda rec: rec["market"].split("/")[1].strip())
        .addfield("racecourse_and_datetime", lambda rec: rec["market_detail"].split(" : ")[0].strip())
        .addfield("place_detail", lambda rec: rec["market_detail"].split(" : ")[1].strip())
        .addfield("racecourse", lambda rec: re.split(r" (?=\d)", rec["racecourse_and_datetime"])[0].strip())
        .select(lambda rec: rec["sport"] == "Horse Racing")
        .addfield("is_place_market", lambda rec: "TBP" in rec["place_detail"] or "Placed" in rec["place_detail"])
        .addfield("places", lambda rec: 1 if not rec["is_place_market"] else int(re.search(r"\d+", rec["place_detail"]).group()))
        .addfield("race_description", lambda rec: rec["place_detail"] if not rec["is_place_market"] else None)
        .convert("race_datetime", lambda x: pendulum.from_format(x, "DD-MMM-YY HH:mm", tz="UTC"))
        .cutout("market", "market_detail", "racecourse_and_datetime", "place_detail", "is_place_market", "settled_date", "sport")
        .dicts()
    )
    return [MongoBetfairHorseracePnl(**rec) for rec in transformed_data]

@task(tags=["Betfair"])
def validate_betfair_pnl_data(data: petl.Table) -> bool:
    header = (
        "\ufeffMarket",
        "Start time",
        "Settled date",
        "Profit/Loss (£)"
    )

    validate_betfair_date = lambda x: re.match(r"\d{2}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}", x)

    constraints = [
        {"name": "market_str", "field": "\ufeffMarket", "assertion": str},
        {"name": "start_time_valid", "field": "Start time", "assertion": validate_betfair_date},
        {"name": "settled_date_valid", "field": "Settled date", "assertion": validate_betfair_date},
        {"name": "pnl_float", "field": "Profit/Loss (£)", "assertion": float},
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def betfair_transformer():
    csv = get_csv()
    data = read_csv(csv)
    problems = validate_betfair_pnl_data(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_betfair_pnl_data(data)


if __name__ == "__main__":
    data = betfair_transformer()  # type: ignore
    print(data)
    
