from typing import Callable, ClassVar, List

import petl
from helpers import read_file
from prefect import get_run_logger
from transformers.theracingapi_transformer import TheRacingApiTransformer

from processors.database_processors import DeclarationProcessor
from processors.processor import Processor


class TheRacingApiFileProcessor(Processor):
    _descriptor: ClassVar[str] = "theracingapi file"
    _forward_processors: ClassVar[List["Processor"]] = [DeclarationProcessor()]

    def __init__(self):
        super().__init__()
        self.file_count = 0

    def process(self, file: str, _callback: Callable):
        logger = get_run_logger()
        logger.info(f"Processing theracingapi {file}")
        
        d = self.running_processors[0]
        
        contents = read_file(file)
        decs = petl.fromdicts([{k: v for k, v in dec.items() if k != "off_dt"} for dec in contents["racecards"]]) 
        data = TheRacingApiTransformer(decs).transform()    

        for item in data:
            d.send(item)
            
        self.file_count += 1

    @property
    def _exit_message(self):
        return f"Finished theracingapi processing. Processed {self.file_count} files."
