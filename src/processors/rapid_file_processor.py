from typing import Callable, ClassVar, List

import petl
from helpers import read_file
from prefect import get_run_logger
from transformers.rapid_horseracing_transformer import RapidHorseracingTransformer

from processors.database_processors import ResultProcessor
from processors.processor import Processor


class RapidFileProcessor(Processor[str]):
    _descriptor: ClassVar[str] = "RapidAPI file"
    _forward_processors: ClassVar[List["Processor"]] = [ResultProcessor()]

    def __init__(self):
        super().__init__()
        self.file_count = 0

    def process(self, file: str, _callback: Callable):
        logger = get_run_logger()
        logger.info(f"Processing RapidAPI {file}")
        
        d = self.running_processors[0]
        
        contents = read_file(file)
        decs = petl.fromdicts([contents]) 
        data = RapidHorseracingTransformer(decs).transform()    

        for item in data:
            d.send(item)
            
        self.file_count += 1

    @property
    def _exit_message(self):
        return f"Finished RapidAPI processing. Processed {self.file_count} files."
