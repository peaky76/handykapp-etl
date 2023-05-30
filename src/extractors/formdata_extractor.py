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


def is_dist_going(string: str) -> str:
    dist_going_regex = r"\d\.?\d?[H|F|M|G|D|S|V|f|g|d|s]"
    return bool(re.match(dist_going_regex, string))


def is_race_date(string: str) -> str:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


@task(tags=["Racing Research"])
def fetch_words():
    doc = fitz.open("src/extractors/textAf.pdf")
    words = []
    for page in doc:
        text = page.get_text()
        for word in text.split("\n"):
            if (word.upper() != word and "-" in word) or len(word) <= 6:
                for subword in word.split(" "):
                    words.append(subword)
            else:
                words.append(word)

    return words


@flow
def formdata_extractor():
    print(fetch_words()[:10000])


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
