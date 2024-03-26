from .race import Race
from .entry import Entry


class Declaration(Race):
    runners: list[Entry]