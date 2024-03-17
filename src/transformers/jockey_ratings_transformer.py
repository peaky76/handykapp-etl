# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


from typing import List

import petl  # type: ignore
import tomllib
from helpers import read_file
from models import JockeyRatings
from nameparser import HumanName  # type: ignore
from prefect import task

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["core"]["spaces_dir"]


@task(tags=["Core"])
def transform_jockey_ratings() -> List[JockeyRatings]:
    filename = f"{SOURCE}jockeys/jockey_ratings_historic.csv"
    data = read_file(filename)

    jockey_ratings = []
    for row in petl.data(data).dicts():
        name_elements = row["Name"].split(" ")
        inverted_name = " ".join([*name_elements[1:], name_elements[0]])
        name = str(HumanName(inverted_name))
        ratings = {
            year: rating
            for year, rating in row.items()
            if len(year) == 4 and year.isdigit()
        } 

        jockey_ratings.append(JockeyRatings(name=name, ratings=ratings))


    return jockey_ratings


if __name__ == "__main__":
    print("Cannot run jockey_ratings_transformer.py as a script.")
