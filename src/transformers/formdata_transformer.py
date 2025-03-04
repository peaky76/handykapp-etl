# To allow running as a script, need path
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import (
    AgeRestriction,
    Going,
    Horselength,
    JumpCategory,
    RaceDistance,
    RaceGrade,
    RaceWeight,
    RacingCode,
)  # type: ignore
from peak_utility.names.corrections import eirify
from prefect import get_run_logger

from helpers import get_files
from models import FormdataHorse, FormdataRun, MongoRace, PreMongoRunner

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]


def create_horse(words: list[str], year: int) -> FormdataHorse | None:
    logger = get_run_logger()
    words = [w for w in words if w]  # Occasional lines have empty strings at end

    name = words[0].split("(")[0].strip()
    country = words[0].split("(")[1].split(")")[0] if "(" in words[0] else "GB"

    try:
        if len(words) == 5:
            # Base case
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(words[1]),
                trainer=eirify(words[2]),
                trainer_form=words[3],
                prize_money=words[4],
                runs=[],
            )
        elif 2 <= len(words) < 5:
            # Handle cases where horse line has been insufficiently split
            further_split = ("".join(words[1:])).split(" ")
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(further_split[0]),
                trainer=eirify(" ".join(further_split[1:-2])),
                trainer_form=further_split[-2],
                prize_money=further_split[-1],
                runs=[],
            )
        else:
            # Handle cases where trainer name has been split
            horse = FormdataHorse(
                name=name,
                country=country,
                year=year - int(words[1]),
                trainer=eirify("".join(words[2:-2])),
                trainer_form=words[-2],
                prize_money=words[-1],
                runs=[],
            )

        if horse.trainer[0].isdigit():
            logger.error(f"Trainer error on {horse.name}: {horse.trainer}")

    except Exception as e:
        logger.error(f"Error creating horse from {words}: {e}")
        horse = None

    return horse


def create_run(words: list[str]) -> FormdataRun | None:
    logger = get_run_logger()
    try:
        # Handle extra empty string at end of some lines
        words = words if words[-1] != "" else words[:-1]

        # Handle cases where words are insufficiently split
        words = " ".join(words).split(" ")

        # Handle odd case of Phoenix Dawn (missing data)
        if len(words) == 10 and words[6:] == ["b", "RHavlin", "p", "p12"]:
            words = words[:9] + ["", "p12", "16d", "p12"]

        # Handle odd case of Arctic Mountain (incorrect data - says placed when dsq)
        if len(words) == 12 and words[7] == "14p940.0":
            words = words[:7] + ["14d", "40.0"] + words[8:]

        # Split overlong prize money
        if len(words[1]) > 5:
            racetype, prize = extract_prize(words[1]) or ("", "")
            words[1] = racetype
            words.insert(2, prize)

        # Split conjoined weight
        if (len(words[5]) > 5 and words[5][0].isdigit() and "-" in words[5][:3]) or (
            len(words[5]) == 5 and not words[5][-1].isdigit()
        ):
            weight, jockey = extract_weight(words[5]) or ("", "")
            words[5] = weight
            words.insert(6, jockey)

        # Split conjoined finishing distance
        for i, word in enumerate(words[7:10]):
            if "p" in word and ("*" in word or "." in word):
                position, beaten_distance = (
                    word.split("*") if "*" in word else (word[:3], word[3:])
                )
                words[7 + i] = position
                words.insert(8 + i, f"-{beaten_distance}")

        # Join non-finishing jump_details
        indices_to_join = []
        for i, word in enumerate(words[-5:-1]):
            if word in ["-", "n", "o"] and words[-5 + i + 1] in ["b", "c", "h"]:
                indices_to_join.append(-5 + i)

        for index in indices_to_join:
            words = (
                words[:index]
                + ["".join(words[index : index + 2])]
                + (words[index + 2 :] if index < -3 else [])
            )

        # Join jockey details to be processed separately
        flat_non_finisher = any(
            letter in words[-4] for letter in ["b", "f", "n", "p", "u"]
        )
        joined_middle = "".join(
            words[6:-3] if flat_non_finisher and words[-4] != "alone" else words[6:-4]
        )
        middle_details = extract_middle_details(joined_middle)

        if not middle_details:
            raise ValueError(f"Insufficient detail in middle of run: {middle_details}")

        (dist, going) = extract_dist_going(words[-2]) or (None, None)

        if not dist or not going:
            raise ValueError("Insufficient detail in distance or going")

        run = FormdataRun(
            date=pendulum.from_format(words[0], "DDMMMYY").date().format("YYYY-MM-DD"),
            race_type=words[1],
            win_prize=words[2],
            course=words[3],
            number_of_runners=int(words[4]),
            weight=words[5],
            headgear=middle_details["headgear"],
            allowance=middle_details["allowance"],
            jockey=middle_details["jockey"],
            position=middle_details["position"],
            beaten_distance=float(words[-4].replace("*", "-"))
            if "." in words[-4] and words[-4] != "w.o."
            else None,
            time_rating=extract_rating(words[-3]),
            distance=dist,
            going=going,
            form_rating=extract_rating(words[-1]),
        )
    except Exception as e:
        logger.error(f"Error creating run from {words}: {e}")
        run = None

    return run


def extract_dist_going(string: str) -> tuple[float, str] | None:
    pattern = r"""
        ^                                       # Start of the string
        (?:r)?                                  # Optional r
        (?P<dist>\d\.?\d?)                      # Distance
        (?P<going>[H|F|M|G|D|S|V|f|m|g|d|s])    # Going
        $                                       # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        dist = match.group("dist")
        going = match.group("going")
        return (float(dist), going)

    return None


def extract_grade(race_type: str) -> str | None:
    pattern = r"""
        G           # Literal 'G'
        [123]       # Single digit 1, 2, or 3
        (?![0-9])   # Not followed by another digit
    """

    match = re.search(pattern, race_type, re.VERBOSE)
    return match.group(0) if match else None


def extract_middle_details(details: str) -> dict | None:
    pattern = r"""
        ^                                       # Start of the string
        (?P<headgear>[a-z])?                    # Lowercase letter as the headgear
        (?P<allowance>\d+)?                     # Optional number as the allowance
        (?P<jockey>[a-zA-Z\-\']+)               # Remaining characters as the jockey
        (?P<position>(\=?\d+(?:p\d+|d)?|[a-z])) # Last number or char as the position (With optional = or #p# formatting)
        $                                       # End of the string
    """

    match = re.match(pattern, details, re.VERBOSE)
    if match:
        return {
            "headgear": match.group("headgear"),
            "allowance": int(match.group("allowance") or 0),
            "jockey": match.group("jockey"),
            "position": match.group("position"),
        }

    return None


def extract_prize(string: str) -> tuple[str, str] | None:
    pattern = r"""
        ^                                   # Start of the string
        (?P<racetype>\d*[A-Za-z]+)?         # Race type
        (?P<prize>\d{3,4})                  # Prize money
        $                                   # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        racetype = match.group("racetype")
        prize = match.group("prize")
        return (racetype, prize)

    return None


def extract_rating(string: str) -> int | None:
    pattern = r"""
        ^                                   # Start of the string
        (?P<disaster>[a-z-]?)               # Disaster
        (?P<rating>\d{1,3})                 # Rating
        (?P<jumps_category>([a-z]-?)?)      # Jumps category
        $                                   # End of the string
    """
    match = re.match(pattern, string, re.VERBOSE)
    if match:
        return int(match.group("rating")) if not match.group("disaster") else None

    return None


def extract_weight(string: str) -> tuple[str, str] | None:
    pattern = r"""
        ^                                   # Start of the string
        (?P<weight>\d{1,2}\-\d{2})          # Weight
        (?P<jockey>.*)                      # Jockey
        $                                   # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        weight = match.group("weight")
        jockey = match.group("jockey")
        return (weight, jockey)

    return None


def get_formdata_date(filename: str) -> pendulum.Date:
    return pendulum.from_format(filename[-10:-4], "YYMMDD").date()


def get_formdatas(
    *, code: RacingCode | None = None, after_year: int = 0, for_refresh: bool = False
) -> list[str]:
    base = [file for file in get_files(SOURCE) if int(file[-10:-8]) > after_year]
    flat = [file for file in base if "nh" not in file]
    nh = [file for file in base if "nh" in file]

    def select_for_refresh(files):
        if not for_refresh:
            return files

        selected_files = []
        next_date = get_formdata_date(files[0])
        current_file = files[0]
        for file in files:
            if get_formdata_date(file) > next_date:
                selected_files.append(current_file)
                next_date = get_formdata_date(current_file).add(years=1)
            current_file = file
        if files[-1] not in selected_files:
            selected_files.append(files[-1])

        return selected_files

    if code == RacingCode.FLAT:
        return select_for_refresh(flat)
    if code == RacingCode.NH:
        return select_for_refresh(nh)

    return select_for_refresh(flat) + select_for_refresh(nh)


def is_horse(string: str) -> bool:
    horse_regex = r"^(?!.*FORMDATA)[A-Z\s]{3,21}(?: \([A-Z]{1,3}\))?$"
    initial_regex = r"^[A-Z] [A-Z] [A-Z]$"
    return (
        string == string.upper()
        and bool(re.match(horse_regex, string))
        and not bool(re.match(initial_regex, string))
    )


def is_race_date(string: str) -> bool:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


def transform_horse(data) -> PreMongoRunner:
    return (
        petl.convert(
            data,
            {
                "weight": lambda x: RaceWeight(x).lb,
                "beaten_distance": lambda x: str(Horselength(x)) if x else None,
            },
        )
        .rename(
            {
                "weight": "lbs_carried",
            }
        )
        .cutout("time_rating", "form_rating")
        .dicts()[0]
    )


def transform_races(data) -> MongoRace:
    return (
        petl.rename(
            data,
            {
                "date": "datetime",
                "distance": "distance_description",
                "going": "going_description",
                "win_prize": "prize",
            },
        )
        .convert(
            {
                "distance_description": lambda x: RaceDistance(furlong=x),
                "going_description": lambda x: Going[x],  # type: ignore
            }
        )
        .addfield(
            "is_handicap",
            lambda rec: "H" in rec["race_type"],
            index=4,
        )
        .addfield(
            "surface",
            lambda rec: "AW"
            if (gd := rec["going_description"]) == gd.lower()
            else "Turf",
        )
        .addfield(
            "obstacle",
            lambda rec: JumpCategory["h"]  # type: ignore
            if "h" in rec["race_type"]
            else JumpCategory["c"]  # type: ignore
            if "c" in rec["race_type"]
            else None,
        )
        .addfield(
            "code",
            lambda rec: RacingCode["NH"]
            if rec["obstacle"] or "b" in rec["race_type"]
            else RacingCode["Flat"],  # type: ignore
        )
        .addfield(
            "age_restriction",
            lambda rec: AgeRestriction("2yo")
            if "2" in rec["race_type"] and "G2" not in rec["race_type"]
            else AgeRestriction("3yo")
            if "3" in rec["race_type"] and "G3" not in rec["race_type"]
            else AgeRestriction("4yo")
            if "4" in rec["race_type"]
            else None,
        )
        .addfield(
            "race_grade",
            lambda rec: str(RaceGrade(extract_grade(rec["race_type"]))),
            index=5,
        )
        .convert("runners", lambda x: [transform_horse(petl.fromdicts([h])) for h in x])
        .cutout("number_of_runners", "race_type")
        .dicts()
    )


def validate_races(data):
    return petl.validate(data, {})


if __name__ == "__main__":
    print("Cannot run formdata_transformer.py as a script.")
