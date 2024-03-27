from .entry import Entry
from .race import Race


class Declaration(Race):
    runners: list[Entry]