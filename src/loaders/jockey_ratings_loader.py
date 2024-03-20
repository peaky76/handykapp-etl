# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow
from processors.database_processors import PersonProcessor
from transformers.jockey_ratings_transformer import JockeyRatingsTransformer

from loaders.loader import Loader


@flow
def load_jockey_ratings():
    data = JockeyRatingsTransformer().transform()
    loader = Loader(data, PersonProcessor())
    loader.load()

if __name__ == "__main__":
    load_jockey_ratings()
