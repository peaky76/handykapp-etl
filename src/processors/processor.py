from typing import Optional

from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError


class Processor:
    _descriptor: str | None = None
    _next_processor: Optional["Processor"] = None

    def update(self, item, source):
        raise NotImplementedError

    def insert(self, item, source):
        raise NotImplementedError

    def process(self):
        added = 0
        updated = 0
        skipped = 0

        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor} processor")
                
        if self._next_processor:
            n = self._next_processor()
            next(n)
        else:
            n = None

        try:
            while True:
                item, source = yield

                if (db_item := self.update(item, source)):
                    logger.debug(f"{item} updated")
                    updated += 1
                else:
                    try:
                        db_item = self.insert(item, source)
                        logger.debug(f"{item} added to db")
                        added += 1
                    except DuplicateKeyError:
                        logger.warning(f"Duplicate {self._descriptor}: {item}")
                        skipped += 1
                    except ValueError as e:
                        logger.warning(e)
                        skipped += 1

                self.post_process(item, db_item["_id"], source, logger)

        except GeneratorExit:
            logger.info(
                f"Finished {self._descriptor} processing. Updated {updated}, added {added}, skipped {skipped}"
            )
