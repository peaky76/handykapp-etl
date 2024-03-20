from abc import ABC
from typing import Callable, Generic, List, TypeVar

import petl
from helpers import log_validation_problem

T = TypeVar("T")

class Transformer(Generic[T], ABC):
    def __init__(self, source_data: petl.Table, validator: Callable, transformer: Callable):
        self.source_data = source_data
        self.validator = validator
        self.transformer = transformer

    def transform(self) -> List[T]:
        if (problems := self.validator(self.source_data)):
            for problem in problems.dicts():
                log_validation_problem(problem)
        return self.transformer(self.source_data)