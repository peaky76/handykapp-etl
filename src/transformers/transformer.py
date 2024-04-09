from abc import ABC
from typing import Callable, Generic, List, TypeVar

import petl
from prefect import get_run_logger

T = TypeVar("T")


def log_validation_problem(problem):
    msg = f"{problem['error']} in row {problem['row']} for {problem['field']}: {problem['value']}"
    logger = get_run_logger()
    logger.warning(msg)

class Transformer(Generic[T], ABC):
    def __init__(self, source_data: petl.Table, validator: Callable, transformer: Callable):
        self.source_data = source_data
        self.validator = validator
        self.transformer = transformer

    def transform(self) -> List[T]:
        logger = get_run_logger()

        if (problems := self.validator(self.source_data)):
            if len(problems.dicts()) > 10:
                logger.warning("More than 10 validation problems with this file")
            elif len(problems.dicts()) > 0:
                for problem in problems.dicts():
                    log_validation_problem(problem)

        try:
            return self.transformer(self.source_data)
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            return []