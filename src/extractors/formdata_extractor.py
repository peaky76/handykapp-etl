# To allow running as a script, need path
from collections import namedtuple
from pathlib import Path
import fitz
import re
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger, task

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
        # Split overlong prize money
        if len(words[1]) > 4:
            racetype, prize = extract_prize(words[1])
            words[1] = racetype
            words.insert(2, prize)
        # Split conjoined weight
        if len(words[5]) > 5 and words[5][0].isdigit() and "-" in words[5][:3]:
            weight, jockey = extract_weight(words[5])
            words[5] = weight
            words.insert(6, jockey)
        # Join jockey details to be processed separately
        middle_details = parse_middle_details("".join(words[6:-4]))

        run = Run(
            *words[:6],
            *middle_details.values(),
            *words[-4:-2],
            *extract_dist_going(words[-2]),
            words[-1],
        )
    except Exception as e:
        logger.error(f"Error creating run from {words}: {e}")
        run = None

    return run


def extract_dist_going(string: str) -> tuple[str]:
    pattern = r"""
        ^                                       # Start of the string
        (?:r)?                                  # Optional r
        (?P<dist>\d\.?\d?)                      # Distance
        (?P<going>[H|F|M|G|D|S|V|f|g|d|s])      # Going
        $                                       # End of the string
    """

    match = re.match(pattern, string, re.VERBOSE)
    if match:
        dist = match.group("dist")
        going = match.group("going")
        return (dist, going)
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


def extract_weight(string: str) -> tuple[str]:
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


def parse_middle_details(details: str) -> list[str]:
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
            "allowance": match.group("allowance"),
            "jockey": match.group("jockey"),
            "position": match.group("position"),
        }
    else:
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

        # Handle then moved to O'Callaghan weirdness
        if "20'Callaghan" in word:
            word = word.split("20'Callaghan")[0]

        if is_horse(word):
            adding_horses = True
            adding_runs = False
            if len(run_args):
                run = create_run(run_args)
                horse.runs.append(run)
                run_args = []

        elif is_race_date(word):
            adding_horses = False
            adding_runs = True
            if len(horse_args):
                horse = create_horse(horse_args)
                horses.append(horse)
                horse_args = []
            if len(run_args):
                run = create_run(run_args)
                horse.runs.append(run)
                run_args = []

        elif "then" in word:
            adding_horses = False
            adding_runs = False

        if adding_horses:
            horse_args.append(word)
        elif adding_runs:
            run_args.append(word)

    logger.info(
        f"Processed {len([h for h in horses if h is not None])} horses from Formdata"
    )
    print(horses[-1])


def stream_formdata_by_word():
    doc = fitz.open("src/extractors/textAf.pdf")
    for page in doc:
        text = page.get_text()
        # Replace non-ascii characters with apostrophes
        words = text.replace(chr(25), "'").replace(chr(65533), "'").split("\n")
        yield from words


@flow
def formdata_extractor():
    word_iterator = stream_formdata_by_word()
    process_formdata_stream(word_iterator)


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
