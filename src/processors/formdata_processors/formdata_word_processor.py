from typing import Any, ClassVar, List

from prefect import get_run_logger
from processors.database_processors import FormdataProcessor
from processors.processor import Processor
from transformers.formdata_transformer import (
    create_horse,
    create_run,
    is_horse,
    is_race_date,
)


class FormdataWordProcessor(Processor):
    _descriptor: ClassVar[str] = "word"
    _forward_processors: ClassVar[List[Processor]] = [FormdataProcessor()]

    def __init__(self):
        super().__init__()
        self.horse = None
        self.horse_args = []
        self.run_args = []
        self.horse_switch = None
        self.run_switch = None
        self.adding_horses = False
        self.adding_runs = False
        self.skip_count = 0

    def process(self, item: Any):
        logger = get_run_logger()
        f = self.running_processors[0]

        word, date = item
        if "FORMDATA" in word:
            self.skip_count = 3
            return

        if self.skip_count > 0:
            self.skip_count -= 1
            return

        self.horse_switch = is_horse(word)
        self.run_switch = is_race_date(word)

        # Switch on/off adding horses/runs
        if self.horse_switch:
            self.adding_horses = True
            self.adding_runs = False
        elif self.run_switch:
            self.adding_horses = False
            self.adding_runs = True
        elif "then" in word:
            self.adding_horses = False
            self.adding_runs = False

        # Create horses/runs
        if self.run_switch and len(self.horse_args):
            self.horse = create_horse(self.horse_args, date.year)
            self.horse_args = []

        if (self.horse_switch or self.run_switch) and len(self.run_args):
            run = create_run(self.run_args)
            if run is None:
                logger.error(f"Missing run for {self.horse.name}")
            else:
                self.horse.runs.append(run)
            self.run_args = []

        # Add horses/runs to db
        if self.horse_switch and self.horse:
            f.send(self.horse)
            # horse_loader.send((horse, date))
            self.horse = None

        # Add words to horses/runs
        if self.adding_horses:
            self.horse_args.append(word)
        elif self.adding_runs:
            self.run_args.append(word)
