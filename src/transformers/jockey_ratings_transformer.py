# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import petl
import tomllib
from helpers import read_file
from nameparser import HumanName

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["core"]["spaces_dir"]


def transform_jockey_ratings():
    filename = f"{SOURCE}jockeys/jockey_ratings_historic.csv"
    data = read_file(filename)

    jockey_ratings = {}
    for row in petl.data(data).dicts():
        name_elements = row["Name"].split(" ")
        inverted_name = " ".join([*name_elements[1:], name_elements[0]])
        name = str(HumanName(inverted_name))
        if row["2022"]:
            jockey_ratings[name] = row["2022"]

    return jockey_ratings

    
if __name__ == "__main__":
    print("Cannot run jockey_ratings_transformer.py as a script.")