from typing import List

from processors.database_processors.database_processor import DatabaseProcessor
from models.hashable_base_model import HashableBaseModel


class Loader:
    def __init__(self, data: List[HashableBaseModel], processor: DatabaseProcessor):
        self.data = data
        self.processor = processor

    def load(self):
        p = self.processor.start()
        next(p)

        for item in self.data:
            p.send(item)

        p.close()