# To allow running as a script, need path
from collections import namedtuple
from pathlib import Path
import fitz
import re
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, task

Horse = namedtuple(
    "Horse", ["name", "country", "age", "trainer", "trainer_form", "prize_money"]
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


def extract_run(string: str) -> str:
    elements = string.split(" ")
    next_date_index = elements.index(next(e for e in elements if is_race_date(e)))
    next_dist_going_index = elements.index(
        next(e for e in elements[next_date_index:] if is_dist_going(e))
    )
    return elements[next_date_index : next_dist_going_index + 2]


def extract_title(words: list[str]):
    for i, word in enumerate(words):
        if "FORMDATA" in word:
            return " ".join(words[i : i + 4])
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
    horse_count = 0
    for word in stream:
        if "FORMDATA" in word:
            print("TITLE")
        elif is_horse(word):
            print(f"HORSE: {word}")
            horse_count += 1
        elif is_race_date(word):
            print("RUN")
    print(f"Total horses: {horse_count}")


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
