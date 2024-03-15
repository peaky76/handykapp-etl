from typing import Any, ClassVar, List

from prefect import get_run_logger


class Processor:
    _next_processors: ClassVar[List["Processor"]] = []

    def __call__(self):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor or 'anonymous'} processor")

        running_processors = []        
        for processor in self._next_processors:
            p = processor()()
            next(p)
            running_processors.append(p)
            
        try:
            while True:
                item = yield
                self.process(item, running_processors)  

        except GeneratorExit:
            logger.info(self._exit_message)
            for processor in running_processors:
                processor.close()

    def process(self, item: Any, running_processors: List["Processor"]):    
        raise NotImplementedError

    @property
    def _descriptor(self):
        name = self.__class__.__name__
        return name.replace("Processor", "").lower() if "Processor" in name else None 

    @property
    def _exit_message(self):
        return f"Finished {self._descriptor or 'item'} processing."