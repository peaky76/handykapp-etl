import re
import sys
from pathlib import Path

import pendulum
import petl

sys.path.append(str(Path(__file__).resolve().parent.parent))

import tomllib
from prefect import flow, get_run_logger, task
from pybet import Odds

from helpers import get_files, log_validation_problem, stream_file
from models.mongo_betfair_horserace_bet_history import MongoBetfairHorseraceBetHistory
from models.mongo_betfair_horserace_pnl import MongoBetfairHorseracePnl

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["betfair"]["spaces_dir"]


@task(tags=["Betfair"])
def get_csv(date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(get_files(SOURCE))
        if "PandL" in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["Betfair"])
def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source, encoding="utf-8")


@task(tags=["Betfair"])
def transform_betfair_bet_history(data: petl.Table) -> list[MongoBetfairHorseraceBetHistory]:
    transformed_data = (
        petl.rename(data, {
            "Market": "market",
            "Selection": "horse",
            "Bid Type": "bet_type",
            "Avg. odds matched": "odds",
            "Stake (£)": "stake",
            "Profit/Loss (£)": "profit_loss"
        })
        .convert("odds", lambda x: Odds(x))
        .convert("stake", float)
        .convert("profit_loss", lambda x: float(x.replace("(", "-").replace(")", "")))
        .addfield("sport", lambda rec: rec["market"].split("/")[0].strip())
        .addfield("market_detail", lambda rec: rec["market"].split("/")[1].strip())
        .addfield("racecourse_and_datetime", lambda rec: rec["market_detail"].split(" : ")[0].strip())
        .addfield("place_detail", lambda rec: rec["market_detail"].split(" : ")[1].strip())
        .addfield("racecourse", lambda rec: re.split(r" (?=\d)", rec["racecourse_and_datetime"])[0].strip())
        .select(lambda rec: rec["sport"] == "Horse Racing")
        .addfield("is_place_market", lambda rec: "TBP" in rec["place_detail"] or "Placed" in rec["place_detail"])
        .addfield("places", lambda rec: 1 if not rec["is_place_market"] else int(re.search(r"\d+", rec["place_detail"]).group()) if "TBP" in rec["place_detail"] else None)
        .addfield("race_description", lambda rec: rec["place_detail"] if not rec["is_place_market"] else None)
        .cutout("market", "market_detail", "racecourse_and_datetime", "place_detail", "is_place_market")
        .dicts()
    )

    filled_out_data = []
    for rec in transformed_data:
        if not rec["race_description"]:    
            matching_description = next((other_rec["race_description"] for other_rec in transformed_data if other_rec["race_datetime"] == rec["race_datetime"] and other_rec["race_description"]), None)
            rec["race_description"] = matching_description
        filled_out_data.append(rec)

    return [MongoBetfairHorseraceBetHistory(**rec) for rec in filled_out_data]


@task(tags=["Betfair"])
def validate_betfair_bet_history(data: petl.Table) -> bool:
    header = (
        "\ufeffMarket",
        "Selection",
        "Bid type",
        "Avg. odds matched",
        "Stake (£)",
        "Profit/Loss (£)"
    )

    validate_betfair_date = lambda x: re.match(r"\d{2}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}", x)

    constraints = [
        {"name": "market_str", "field": "\ufeffMarket", "assertion": str},
        {"name": "horse_str", "field": "Selection", "assertion": str},
        {"name": "bid_type_str", "field": "Bid type", "assertion": lambda x: x in ("BACK", "LAY")},
        {"name": "odds_float", "field": "Avg. odds matched", "assertion": float},
        {"name": "stake_float", "field": "Stake (£)", "assertion": float},
        {"name": "start_time_valid", "field": "Start time", "assertion": validate_betfair_date},
        {"name": "pnl_float", "field": "Profit/Loss (£)", "assertion": lambda x: x == 0.00 or float(x)},
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["Betfair"])
def transform_betfair_pnl_data(data: petl.Table) -> list[MongoBetfairHorseracePnl]:
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
        .addfield("places", lambda rec: 1 if not rec["is_place_market"] else int(re.search(r"\d+", rec["place_detail"]).group()) if "TBP" in rec["place_detail"] else None)
        .addfield("race_description", lambda rec: rec["place_detail"] if not rec["is_place_market"] else None)
        .convert("race_datetime", lambda x: pendulum.from_format(x, "DD-MMM-YY HH:mm", tz="UTC"))
        .cutout("market", "market_detail", "racecourse_and_datetime", "place_detail", "is_place_market", "settled_date", "sport")
        .dicts()
    )

    filled_out_data = []
    for rec in transformed_data:
        if not rec["race_description"]:    
            matching_description = next((other_rec["race_description"] for other_rec in transformed_data if other_rec["race_datetime"] == rec["race_datetime"] and other_rec["race_description"]), None)
            rec["race_description"] = matching_description
        filled_out_data.append(rec)

    return [MongoBetfairHorseracePnl(**rec) for rec in filled_out_data]


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
        {"name": "pnl_float", "field": "Profit/Loss (£)", "assertion": lambda x: x == 0.00 or float(x)},
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def betfair_transformer():
    logger = get_run_logger()
    csv = get_csv()
    data = read_csv(csv)
    problems = validate_betfair_pnl_data(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_betfair_pnl_data(data)


if __name__ == "__main__":
    data = betfair_transformer()  # type: ignore
    print(data)
    
