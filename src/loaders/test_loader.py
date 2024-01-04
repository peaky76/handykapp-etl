# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import write_file
from prefect import flow


@flow
def test_load():
    write_file("test successful", 'handykapp/test.txt')


if __name__ == "__main__":
    test_load()
