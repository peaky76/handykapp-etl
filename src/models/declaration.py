from .race import Race
from .runner import Runner


class Declaration(Race):
    runners: list[Runner]