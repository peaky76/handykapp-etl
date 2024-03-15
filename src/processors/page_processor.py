from typing import Any, ClassVar, List

from .processor import Processor
from .word_processor import WordProcessor


class PageProcessor(Processor):
    _next_processors: ClassVar[List["Processor"]] = [WordProcessor]

    def __init__(self):
        self.page_count = 0

    def process(self, item: Any, running_processors: List[Processor]):
        page, date = item
        text = page.get_text()
        
        w = running_processors[0]
        # Replace non-ascii characters with apostrophes
        words = (
            text.replace(f"{chr(10)}{chr(25)}", "'")  # Newline + apostrophe
            .replace(f"{chr(32)}{chr(25)}", "'")  # Space + apostrophe
            .replace(chr(25), "'")  # Regular apostrophe
            .replace(chr(65533), "'")  # Replacement character
            .split("\n")
        )
        for word in words:
            w.send((word, date))