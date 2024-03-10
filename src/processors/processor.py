from functools import cache
from typing import Optional

from models import ProcessBaseModel, PyObjectId
from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError


class Processor:
    _descriptor: str | None = None
    _next_processor: Optional["Processor"] = None
    _table = None
    _search_dictionary = {}
    _update_dictionary = {} 
    _insert_dictionary = {}

    @cache
    def find(self, item: ProcessBaseModel) -> PyObjectId | None:
        found_item = self._table.find_one(self._search_dictionary(item), {"_id": 1})
        return found_item["_id"] if found_item else None


    def update(self, item: ProcessBaseModel, db_id: PyObjectId) -> None:
        self._table.update_one(
            {"_id": db_id},
            {"$set": self._update_dictionary(item)},
        )

    def insert(self, item: ProcessBaseModel) -> PyObjectId:
        return self._table.insert_one(self._insert_dictionary(item))

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
                item = yield

                if (db_id := self.find(item)):
                    self.update(item, db_id)
                    logger.debug(f"{item} updated")
                    updated += 1
                else:
                    try:
                        db_id = self.insert(item)
                        logger.debug(f"{item} added to db")
                        added += 1
                    except DuplicateKeyError:
                        logger.warning(f"Duplicate {self._descriptor}: {item}")
                        skipped += 1
                    except ValueError as e:
                        logger.warning(e)
                        skipped += 1

                self.post_process(item, db_id, logger)

        except GeneratorExit:
            logger.info(
                f"Finished {self._descriptor} processing. Updated {updated}, added {added}, skipped {skipped}"
            )
