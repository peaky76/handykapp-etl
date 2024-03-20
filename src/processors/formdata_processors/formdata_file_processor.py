from typing import ClassVar, List

import fitz  # type: ignore
from helpers import stream_file
from processors.processor import Processor
from transformers.formdata_transformer import get_formdata_date

from .formdata_page_processor import FormdataPageProcessor


class FormdataFileProcessor(Processor):
    _forward_processors: ClassVar[List["Processor"]] = [FormdataPageProcessor()]

    def __init__(self):
        super().__init__()
        self.page_count = 0

    def process(self, file: str):
        date = get_formdata_date(file)
        doc = fitz.open("pdf", stream_file(file))
        p = self.running_processors[0]

        for page in doc:
            p.send((page, date))
            self.page_count += 1

    @property
    def _exit_message(self):
        return f"Finished file processing. Processed {self.page_count} pages."
