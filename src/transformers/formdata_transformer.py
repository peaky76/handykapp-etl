# To allow running as a script, need path
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re
from collections import namedtuple

import pendulum
import tomllib
from helpers import get_files
from horsetalk import RacingCode  # type: ignore
from models import FormdataEntry, FormdataRun, Race
from models.racecourse_fields import ObstacleType
from prefect import get_run_logger

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]


Horse = namedtuple(
    "Horse",
    ["name", "country", "year", "trainer", "trainer_form", "prize_money", "runs"],
)
Run = namedtuple(
    "Run",
    [
        "date",
        "race_type",
        "win_prize",
        "racecourse",
        "number_of_runners",
        "weight",
        "headgear",
        "allowance",
        "jockey",
        "position",
        "beaten_distance",
        "time_rating",
        "distance",
        "going",
        "form_rating",
    ],
)

def extract_obstacle_from_formdata_run(run: FormdataRun) -> ObstacleType | None: 
    if "h" in run.race_type:
        return "Hurdle"
        
    if "c" in run.race_type:
        if run.racecourse == "Chm" and str(int(run.distance)) == "29":
            return "Cross-Country"
        return "Chase"

    return None

def extract_race_from_formdata_run(run: FormdataRun) -> Race:
    # class FormdataRun(HashableBaseModel):
#     date: datetime
#     race_type: str
#     win_prize: str
#     racecourse: str = Field(..., min_length=2, max_length=3)
#     number_of_runners: int
#     weight: Annotated[str, StringConstraints(pattern=r'^(7|8|9|10|11|12)-([0-9]|10|11|12|13)$')]
#     headgear: Optional[str] = None
#     allowance: Optional[int] = None
#     jockey: str
#     position: str
#     beaten_distance: Optional[float] = None
#     time_rating: Optional[int | str]
#     distance: float | str
#     going: Literal["H", "F", "M", "G", "D", "S", "V", "f", "m", "g", "d", "s"]
#     form_rating: Optional[int | str]

# class Race(HashableBaseModel):
#     racecourse: Racecourse
#     datetime: datetime
#     title: str
#     is_handicap: Optional[bool] = None
#     is_cancelled: bool = False
#     distance_description: str
#     race_grade: Optional[str] = None
#     race_class: Optional[int] = None
#     age_restriction: Optional[RaceRestriction] = None
#     rating_restriction: Optional[RaceRestriction] = None
#     prize: Optional[str] = None
#     going_description: Optional[str] = None
#     number_of_runners: Optional[int] = None
#     runners: list[Run]
#     references: Optional[References] = None
#     source: Source

    return {}


def create_horse(words: list[str], year: int) -> FormdataEntry | None:
    logger = get_run_logger()
    words = [w for w in words if w]  # Occasional lines have empty strings at end

    name = words[0].split("(")[0].strip()
    country = words[0].split("(")[1].split(")")[0] if "(" in words[0] else "GB"

    # TODO : Will be in next v0.5 of peak_utility
    irishise = lambda x: x.replace("O' ", "O'").replace("O '", "O'")

    try:
        if len(words) == 5:
            # Base case
            horse = Horse(
                name, country, year - int(words[1]), irishise(words[2]), *words[3:], []
            )
        elif 2 <= len(words) < 5:
            # Handle cases where horse line has been insufficiently split
            further_split = ("".join(words[1:])).split(" ")
            horse = Horse(
                name,
                country,
                year - int(further_split[0]),
                irishise(" ".join(further_split[1:-2])),
                *further_split[-2:],
                [],
            )
        else:
            # Handle cases where trainer name has been split
            horse = Horse(
                name,
                country,
                year - int(words[1]),
                irishise("".join(words[2:-2])),
                *words[-2:],
                [],
            )

        if horse.trainer[0].isdigit():
            logger.error(f"Trainer error on {horse.name}: {horse.trainer}")

    except Exception as e:
        logger.error(f"Error creating horse from {words}: {e}")
        horse = None

    return FormdataEntry(**horse._asdict()) if horse else None


def create_run(words: list[str]) -> FormdataRun:
    logger = get_run_logger()
    try:
        # Handle extra empty string at end of some lines
        words = words if words[-1] != "" else words[:-1]

        # Handle cases where words are insufficiently split
        words = " ".join(words).split(" ")

        # Handle odd case of Phoenix Dawn (missing data)
        if len(words) == 10 and words[6:] == ["b", "RHavlin", "p", "p12"]:
            words = words[:9] + ["", "p12", "16d", "p12"]

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
        for i, word in enumerate(words[7:9]):
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

        run = Run(
            pendulum.from_format(words[0], "DDMMMYY").date().format("YYYY-MM-DD"),
            *words[1:6],
            *middle_details.values(),
            float(words[-4].replace("*", "-"))
            if "." in words[-4] and words[-4] != "w.o."
            else None,
            extract_rating(words[-3]),
            *extract_dist_going(words[-2]),
            extract_rating(words[-1]),
        )
    except Exception as e:
        logger.error(f"Error creating run from {words}: {e}")
        run = None

    return FormdataRun(**run._asdict()) if run else None


def extract_dist_going(string: str) -> tuple[str] | None:
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


def extract_prize(string: str) -> tuple[str] | None:
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


def extract_rating(string: str) -> str | None:
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


def extract_weight(string: str) -> tuple[str] | None:
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
        if len(files):
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


if __name__ == "__main__":
    print("Cannot run formdata_transformer.py as a script.")
