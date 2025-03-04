from prefect import get_run_logger

from transformers.formdata_transformer import (
    create_horse,
    create_run,
    is_horse,
    is_race_date,
)

from .entry_processor import entry_processor
from .race_builder import race_builder


def word_processor():
    logger = get_run_logger()
    logger.info("Starting word processor")
    horse = None
    horse_args = []
    run_args = []
    adding_horses = False
    adding_runs = False

    ep = entry_processor()
    next(ep)
    rb = race_builder()
    next(rb)

    try:
        while True:
            item = yield
            word, date = item
            if "FORMDATA" in word:
                skip_count = 3
                continue

            if skip_count > 0:
                skip_count -= 1
                continue

            horse_switch = is_horse(word)
            run_switch = is_race_date(word)

            # Switch on/off adding horses/runs
            if horse_switch:
                adding_horses = True
                adding_runs = False
            elif run_switch:
                adding_horses = False
                adding_runs = True
            elif "then" in word:
                adding_horses = False
                adding_runs = False

            # Create horses/runs
            if run_switch and len(horse_args):
                horse = create_horse(horse_args, date.year)
                horse_args = []

            if (horse_switch or run_switch) and len(run_args):
                run = create_run(run_args)
                if horse and run is None:
                    logger.error(f"Missing run for {horse.name}")
                elif horse:
                    horse.runs.append(run)
                else:
                    logger.error("Run created but no horse to add it to")
                run_args = []

            # Add horses/runs to db
            if horse_switch and horse:
                ep.send(horse)
                rb.send(horse)
                horse = None

            # Add words to horses/runs
            if adding_horses:
                horse_args.append(word)
            elif adding_runs:
                run_args.append(word)

    except GeneratorExit:
        ep.close()
        rb.close()
