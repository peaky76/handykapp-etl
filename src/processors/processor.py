from prefect import get_run_logger


class Processor:
    def __init__(self, object_name, process_func, next_processor = None):
        self.object_name = object_name
        self.process_func = process_func
        self.next_processor = next_processor

    def process(self):
        logger = get_run_logger()
        logger.info(f"Starting {self.object_name} processor")
        
        if self.next_processor:
            n = self.next_processor()
            next(n)

        try:
            while True:
                item, source = yield
                added, updated, skipped = self.process_func(item, source, logger, n)
        except GeneratorExit:
            logger.info(
                f"Finished processing {self.object_name}. Updated {updated}, added {added}, skipped {skipped}"
            )
