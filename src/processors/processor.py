from typing import Any, ClassVar, List

from prefect import get_run_logger


class Processor:
    _forward_processors: ClassVar[List["Processor"]] = []

    def __init__(self):
        self.running_processors = []

    def start(self):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor or 'anonymous'} processor")
      
        for processor in self._forward_processors:
            p = processor.start()
            next(p)
            self.running_processors.append(p)
            
        try:
            while True:
                item = yield
                self.process(item)  

        except GeneratorExit:
            logger.info(self._exit_message)
            for processor in self.running_processors:
                processor.close()
            self.running_processors = []

    def process(self, item: Any):    
        raise NotImplementedError

    @property
    def _descriptor(self):
        name = self.__class__.__name__
        return name.replace("Processor", "").lower() if "Processor" in name else None 

    @property
    def _exit_message(self):
        return f"Finished {self._descriptor or 'item'} processing."