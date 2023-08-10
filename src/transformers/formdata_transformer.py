# To allow running as a script, need path
from collections import namedtuple
from pathlib import Path
import fitz  # type: ignore
import re
import sys
import pendulum
import yaml

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import get_files, stream_file
from horsetalk import RacingCode
from prefect import flow, get_run_logger, task

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["formdata"]["spaces"]["dir"]


Horse = namedtuple(
    "Horse",
    ["name", "country", "age", "trainer", "trainer_form", "prize_money", "runs"],
)
Run = namedtuple(
    "Run",
    [
        "date",
        "type",
        "win_prize",
        "course",
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


def create_horse(words: list[str]) -> Horse:
    logger = get_run_logger()
    words = [w for w in words if w]  # Occasional lines have empty strings at end

    name = words[0].split("(")[0].strip()
    country = words[0].split("(")[1].split(")")[0] if "(" in words[0] else "GB"

    try:
        if len(words) == 5:
            # Base case
            horse = Horse(name, country, *words[1:], [])
        elif 2 <= len(words) < 5:
            # Handle cases where horse line has been insufficiently split
            further_split = ("".join(words[1:])).split(" ")
            horse = Horse(
                name,
                country,
                further_split[0],
                " ".join(further_split[0:-2]),
                *further_split[-2:],
                [],
            )
        else:
            # Handle cases where trainer name has been split
            horse = Horse(
                name, country, words[1], "".join(words[2:-2]), *words[-2:], []
            )
    except Exception as e:
        logger.error(f"Error creating horse from {words}: {e}")
        horse = None

    return horse


def create_run(words: list[str]) -> Run:
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
            racetype, prize = extract_prize(words[1])
            words[1] = racetype
            words.insert(2, prize)

        # Split conjoined weight
        if (len(words[5]) > 5 and words[5][0].isdigit() and "-" in words[5][:3]) or (
            len(words[5]) == 5 and not words[5][-1].isdigit()
        ):
            weight, jockey = extract_weight(words[5])
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

        # Join jockey details to be processed separately
        middle_details = parse_middle_details("".join(words[6:-4]))

        run = Run(
            pendulum.from_format(words[0], "DDMMMYY").date().format("YYYY-MM-DD"),
            *words[1:6],
            *middle_details.values(),
            float(words[-4].replace("*", "-")) if "." in words[-4] else None,
            int(words[-3]) if words[-3].isdigit() else None,
            *extract_dist_going(words[-2]),
            int(words[-1]) if words[-1].isdigit() else None,
        )
    except Exception as e:
        logger.error(f"Error creating run from {words}: {e}")
        run = None

    return run


def extract_dist_going(string: str) -> tuple[str] | None:
    pattern = r"""
        ^                                       # Start of the string
        (?:r)?                                  # Optional r
        (?P<dist>\d\.?\d?)                      # Distance
        (?P<going>[H|F|M|G|D|S|V|f|m|g|d|s])      # Going
        $                                       # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        dist = match.group("dist")
        going = match.group("going")
        return (float(dist), going)
    else:
        return None


def extract_prize(string: str) -> tuple[str]:
    pattern = r"""
        ^                               # Start of the string
        (?P<racetype>\d*[A-Za-z]+)?     # Race type
        (?P<prize>\d{3,4})              # Prize money
        $                               # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        racetype = match.group("racetype")
        prize = match.group("prize")
        return (racetype, prize)
    else:
        return None


def extract_weight(string: str) -> tuple[str] | None:
    pattern = r"""
        ^                               # Start of the string
        (?P<weight>\d{1,2}\-\d{2})      # Weight
        (?P<jockey>.*)                  # Jockey
        $                               # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        weight = match.group("weight")
        jockey = match.group("jockey")
        return (weight, jockey)
    else:
        return None


def get_formdatas(*, code: RacingCode = None, after_year: int = 0) -> list[str]:
    base = [file for file in get_files(SOURCE) if int(file[-10:-8]) > after_year]
    flat = [file for file in base if "nh" not in file]
    nh = [file for file in base if "nh" in file]

    if code == RacingCode.FLAT:
        return flat
    elif code == RacingCode.NH:
        return nh
    else:
        return flat + nh


def is_horse(string: str) -> str:
    horse_regex = r"^(?!.*FORMDATA)[A-Z\s]{3,18}(?: \([A-Z]{1,3}\))?$"
    initial_regex = r"^[A-Z] [A-Z] [A-Z]$"
    return (
        string == string.upper()
        and bool(re.match(horse_regex, string))
        and not bool(re.match(initial_regex, string))
    )


def is_race_date(string: str) -> str:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


def parse_middle_details(details: str) -> list[str] | None:
    pattern = r"""
        ^                                       # Start of the string
        (?P<headgear>[a-z])?                    # Lowercase letter as the headgear
        (?P<allowance>\d+)?                     # Optional number as the allowance
        (?P<jockey>[a-zA-Z\-\']+)               # Remaining characters as the jockey
        (?P<position>(\=?\d+(?:p\d+|d)?|[a-z]))   # Last number or char as the position (With optional = or #p# formatting)
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
    else:
        print(details)
        return None


def process_formdata_stream(stream):
    logger = get_run_logger()
    horses = []
    horse_args = []
    run_args = []
    adding_horses = False
    adding_runs = False

    for word in stream:
        # Skip any titles
        if "FORMDATA" in word:
            skip_count = 3
            continue
        else:
            if skip_count > 0:
                skip_count -= 1
                continue

        horse_switch = is_horse(word)
        run_switch = is_race_date(word)

        # Switch on/off adding horses/runs
        if horse_switch:
            adding_horses = True
            adding_runs = False
        elif run_switch:
            adding_horses = False
            adding_runs = True
        elif "then" in word:
            adding_horses = False
            adding_runs = False

        # Create horses/runs
        if run_switch and len(horse_args):
            horse = create_horse(horse_args)
            horses.append(horse)
            horse_args = []

        if (horse_switch or run_switch) and len(run_args):
            run = create_run(run_args)
            if run == None:
                logger.error(f"Missing run for {horse.name}")
            else:
                horse.runs.append(run)
            run_args = []

        # Add words to horses/runs
        if adding_horses:
            horse_args.append(word)
        elif adding_runs:
            run_args.append(word)

    return horses


def stream_formdata_by_word(file):
    doc = fitz.open("pdf", stream_file(file))
    for page in doc:
        text = page.get_text()
        # Replace non-ascii characters with apostrophes
        words = (
            text.replace(f"{chr(10)}{chr(25)}", "'")  # Newline + apostrophe
            .replace(f"{chr(32)}{chr(25)}", "'")  # Space + apostrophe
            .replace(chr(25), "'")  # Regular apostrophe
            .replace(chr(65533), "'")
            .split("\n")
        )
        yield from words


@flow
def formdata_transformer():
    files = get_formdatas(code=RacingCode.FLAT, after_year=20)
    logger = get_run_logger()
    logger.info(f"Processing {len(files)} files from {SOURCE}")

    stored_horses = []
    for file in files:
        word_iterator = stream_formdata_by_word(file)
        horses = process_formdata_stream(word_iterator)
        logger.info(
            f"Processed {len([h for h in horses if h is not None])} horses from Formdata"
        )

        if len(stored_horses) == 0:
            stored_horses = horses
        else:
            for horse in horses:
                matched_horse = next(
                    (
                        h
                        for h in stored_horses
                        if h.name == horse.name and h.country == horse.country
                    ),
                    None,
                )
                if matched_horse:
                    runs = matched_horse.runs
                    for new_run in horse.runs:
                        matched_run = next(
                            (r for r in runs if r.date == new_run.date),
                            None,
                        )
                        if matched_run:
                            runs.remove(matched_run)
                        runs.append(new_run)

                    updated_horse = Horse(
                        matched_horse.name,
                        matched_horse.country,
                        horse.age,
                        horse.trainer,
                        horse.trainer_form,
                        horse.prize_money,
                        runs,
                    )
                    stored_horses.remove(matched_horse)
                    stored_horses.append(updated_horse)
                else:
                    stored_horses.append(horse)

    return stored_horses


if __name__ == "__main__":
    formdata_transformer()  # type: ignore
