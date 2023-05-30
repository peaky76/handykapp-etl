# To allow running as a script
from pathlib import Path
import fitz
import re
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, task


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


def is_race_date(string: str) -> str:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


def stream_formdata_by_word():
    doc = fitz.open("src/extractors/textAf.pdf")
    for page in doc:
        text = page.get_text()
        words = text.split("\n")
        yield from words


@flow
def formdata_extractor():
    word_iterator = stream_formdata_by_word()
    for word in word_iterator:
        print(word)


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
