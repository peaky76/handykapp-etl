# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow
from processors.database_processors import PersonProcessor
from transformers.jockey_ratings_transformer import jockey_ratings_transformer

from loaders.loader import Loader


@flow
def load_jockey_ratings():
    loader = Loader(jockey_ratings_transformer(), PersonProcessor())
    loader.load()

if __name__ == "__main__":
    load_jockey_ratings()
