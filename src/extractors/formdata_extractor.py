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
    ["name", "country", "age", "trainer", "trainer_form", "prize_money"],
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
            horse = Horse(name, country, *words[1:])
        elif 2 <= len(words) < 5:
            # Handle cases where horse line has been insufficiently split
            further_split = ("".join(words[1:])).split(" ")
            horse = Horse(
                name,
                country,
                further_split[0],
                " ".join(further_split[0:-2]),
                *further_split[-2:],
            )
        else:
            # Handle cases where trainer name has been split
            horse = Horse(name, country, words[1], "".join(words[2:-2]), *words[-2:])
    except:
        logger.error(f"Error creating horse from {words}")
        horse = None

    print(horse)
    return horse


def create_run(words: list[str]) -> Run:
    words = " ".join(words).split(" ")
    if len(words) == 12:
        dist_and_going = list(words[10])
        dist = "".join(dist_and_going[:-1])
        going = dist_and_going[-1]

        run = Run(*words[:10], dist, going, words[11])
        print(run)
        return run
    else:
        print(words)
        return None


def is_dist_going(string: str) -> str:
    dist_going_regex = r"\d\.?\d?[H|F|M|G|D|S|V|f|g|d|s]"
    return bool(re.match(dist_going_regex, string))


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


def process_formdata_stream(stream):
    horses = []
    horse_args = []
    run_args = []
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

        if is_horse(word):
            adding_runs = False
            if len(run_args):
                create_run(run_args)
                run_args = []

        elif is_race_date(word):
            adding_runs = True
            if len(horse_args):
                horse = create_horse(horse_args)
                horses.append(horse)
                horse_args = []
            if len(run_args):
                create_run(run_args)
                run_args = []

        if not adding_runs:
            horse_args.append(word)
        elif "then moved" in word:
            pass
        else:
            run_args.append(word)

    print(f"Horse count {len([h for h in horses if h is not None])}")


def stream_formdata_by_word():
    doc = fitz.open("src/extractors/textAf.pdf")
    for page in doc:
        text = page.get_text()
        words = text.split("\n")
        yield from words


@flow
def formdata_extractor():
    word_iterator = stream_formdata_by_word()
    process_formdata_stream(word_iterator)


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
