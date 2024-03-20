from processors.database_processors.database_processor import DatabaseProcessor
from transformers.transformer import Transformer


class Loader:
    def __init__(self, transformer: Transformer, processor: DatabaseProcessor):
        self.transformer = transformer
        self.processor = processor

    def load(self):
        data = self.transformer.transform()
        p = self.processor.start()
        next(p)

        for item in data:
            p.send(item)

        p.close()