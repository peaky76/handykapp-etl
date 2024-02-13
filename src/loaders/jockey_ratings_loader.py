# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger
from processors.person_processor import person_processor
from transformers.jockey_ratings_transformer import transform_jockey_ratings


@flow
def load_jockey_ratings():
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    p = person_processor()
    next(p)

    for jockey, rating in transform_jockey_ratings().items():
        p.send(({"name": jockey, "role": "jockey"}, "rr", { "2023": rating }))

    p.close()


if __name__ == "__main__":
    load_jockey_ratings()