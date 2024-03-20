from typing import Any, Callable, ClassVar, List

from processors.processor import Processor

from .formdata_word_processor import FormdataWordProcessor


class FormdataPageProcessor(Processor):
    _descriptor: ClassVar[str] = "page"
    _forward_processors: ClassVar[List["Processor"]] = [FormdataWordProcessor()]

    def __init__(self):
        super().__init__()
        self.page_count = 0

    def process(self, item: Any, _callback: Callable):
        page = item
        text = page.get_text()
        
        w = self.running_processors[0]
        # Replace non-ascii characters with apostrophes
        words = (
            text.replace(f"{chr(10)}{chr(25)}", "'")  # Newline + apostrophe
            .replace(f"{chr(32)}{chr(25)}", "'")  # Space + apostrophe
            .replace(chr(25), "'")  # Regular apostrophe
            .replace(chr(65533), "'")  # Replacement character
            .split("\n")
        )
        for word in words:
            w.send(word)