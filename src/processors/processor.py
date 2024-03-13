from typing import Any, ClassVar, Optional

from prefect import get_run_logger


class Processor:
    _descriptor: ClassVar[Optional[str]] = None
    _next_processor: ClassVar[Optional["Processor"]] = None

    def process_wrapper(self):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor} processor")
                
        if self._next_processor:
            n = self._next_processor()
            next(n)

        try:
            while True:
                item = yield
                self.process(item)  

        except GeneratorExit:
            logger.info(self._exit_message)

    def process(self, item: Any):
        raise NotImplementedError

    def _exit_message(self):
        return f"Finished {self._descriptor} processing."