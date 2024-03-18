from abc import ABC
from typing import Callable, ClassVar, Generator, Generic, List, Optional, TypeVar

# from prefect import get_run_logger

T = TypeVar("T")

class Processor(Generic[T], ABC):
    _forward_processors: ClassVar[List["Processor"]] = []

    def __init__(self):
        self.running_processors = []

    def start(self) -> Generator:
        # logger = get_run_logger()
        # logger.info(f"Starting {self._descriptor or 'anonymous'} processor")
      
        for fp in self._forward_processors:
            p = fp.start()
            next(p)
            self.running_processors.append(p)
            
        try:
            while True:
                incoming = yield
                item, callback = incoming if isinstance(incoming, tuple) else (incoming, lambda x: None)
                self.process(item, callback)  

        except GeneratorExit:
            # logger.info(self._exit_message)
            for rp in self.running_processors:
                rp.close()
            self.running_processors = []

    def process(self, item: T, callback: Callable) -> None:      # noqa: F821
        raise NotImplementedError

    @property
    def _descriptor(self) -> Optional[str]:
        name = self.__class__.__name__
        return name.replace("Processor", "").lower() if "Processor" in name else None 

    @property
    def _exit_message(self) -> str:
        return f"Finished {self._descriptor or 'item'} processing."