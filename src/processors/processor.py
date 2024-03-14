from typing import Any, ClassVar, List, Optional

from prefect import get_run_logger


class Processor:
    _descriptor: ClassVar[Optional[str]] = None
    _next_processors: ClassVar[List["Processor"]] = []

    def process_wrapper(self):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor} processor")
                
        for processor in self._next_processors:
            p = processor()
            next(p)

        try:
            while True:
                item = yield
                self.process(item)  

        except GeneratorExit:
            logger.info(self._exit_message)
            for processor in self._next_processors:
                processor.close()

    def process(self, item: Any):
        raise NotImplementedError

    def _exit_message(self):
        return f"Finished {self._descriptor} processing."