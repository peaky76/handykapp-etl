from .race import Race
from .run import Run


class Result(Race):
    runners: list[Run]