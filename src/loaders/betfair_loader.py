# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

from processors.betfair_processor import betfair_processor
from transformers.betfair_transformer import betfair_transformer


@flow
def load_betfair_horserace_pnl():
    logger = get_run_logger()
    logger.info("Starting betfair loader")

    bf = betfair_processor()
    next(bf)

    for bf_pnl_line in betfair_transformer():
        bf.send(bf_pnl_line)

    bf.close()


if __name__ == "__main__":
    load_betfair_horserace_pnl()
