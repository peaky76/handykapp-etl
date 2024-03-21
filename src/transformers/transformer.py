from abc import ABC
from typing import Callable, Generic, List, TypeVar

import petl
from helpers import log_validation_problem
from prefect import get_run_logger

T = TypeVar("T")

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
                    
        return self.transformer(self.source_data)