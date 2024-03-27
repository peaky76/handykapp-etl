from typing import Callable, ClassVar, List

import fitz  # type: ignore
from helpers import stream_file
from prefect import get_run_logger
from processors.processor import Processor
from transformers.formdata_transformer import get_formdata_date

from .formdata_context import formdata_year
from .formdata_page_processor import FormdataPageProcessor


class FormdataFileProcessor(Processor):
    _descriptor: ClassVar[str] = "file"
    _forward_processors: ClassVar[List[Processor]] = [FormdataPageProcessor()]

    def __init__(self):
        super().__init__()
        self.page_count = 0

    def process(self, file: str, _callback: Callable):
        logger = get_run_logger()
        logger.info(f"Processing formdata {file}")
        date = get_formdata_date(file)
        formdata_year.set(date.year)
        doc = fitz.open("pdf", stream_file(file))
        p = self.running_processors[0]

        for page in doc:
            p.send(page)
            self.page_count += 1

    @property
    def _exit_message(self):
        return f"Finished file processing. Processed {self.page_count} pages."
