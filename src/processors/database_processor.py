from functools import cache
from typing import ClassVar, List, Optional

from models import PyObjectId, TransformedBaseModel
from peak_utility.listish import compact
from prefect import get_run_logger
from pydantic import BaseModel
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from .processor import Processor


class DatabaseProcessor(Processor):
    _descriptor: ClassVar[Optional[str]] = None
    _next_processor: ClassVar[Optional["DatabaseProcessor"]] = None
    _table: ClassVar[Optional[Collection]] = None
    _search_keys: ClassVar[Optional[List[str]]] = None
    _update_keys: ClassVar[Optional[List[str]]] = None
    _insert_keys: ClassVar[Optional[List[str]]] = None
    
    def __init__(self):
        self.added = 0
        self.updated = 0
        self.skipped = 0

    def _search_dictionary(self, item: TransformedBaseModel) -> dict: 
        return compact(item.model_dump(include=self._search_keys) if self._search_keys else item.model_dump())

    def _update_dictionary(self, item: TransformedBaseModel) -> dict:
        return compact(item.model_dump(include=self._update_keys) if self._update_keys else item.model_dump())

    def _insert_dictionary(self, item: TransformedBaseModel) -> dict:
        return compact(item.model_dump(include=self._insert_keys) if self._insert_keys else item.model_dump())

    @cache
    def find(self, item: TransformedBaseModel) -> BaseModel | None:
        return self._table.find_one(self._search_dictionary(item))

    def update(self, item: TransformedBaseModel, db_id: PyObjectId) -> None:
        self._table.update_one(
            {"_id": db_id},
            {"$set": self._update_dictionary(item)},
        )

    def insert(self, item: TransformedBaseModel) -> PyObjectId:
        return self._table.insert_one(self._insert_dictionary(item))

    def process(self, *, find_first: bool = True):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor} processor")
                
        if self._next_processor:
            n = self._next_processor()
            next(n)

        try:
            while True:
                item = yield
                db_id = None

                def update_if_needed(item: TransformedBaseModel, db_item: BaseModel):
                    d = self._update_dictionary(item)
                    if any(db_item[k] != d[k] for k in d):
                        self.update(item, db_item["_id"])
                        logger.debug(f"{item} updated")
                        self.updated += 1
                    else:
                        logger.debug(f"{item} unchanged")
                        self.skipped += 1

                try:
                    if find_first and (db_item := self.find(item)):
                        update_if_needed(item, db_item)
                    else:
                        db_id = self.insert(item)
                        logger.debug(f"{item} added to db")
                        self.added += 1
                except DuplicateKeyError:
                    if not find_first:
                        db_item = self.find(item)
                        update_if_needed(item, db_item)
                        db_id = db_item["_id"]
                except ValueError as e:
                    logger.warning(e)
                    self.skipped += 1
                    
                total = self.updated + self.added + self.skipped
                if total % 250 == 0:
                    logger.info(f"Processed {total} {self._descriptor}s.")

                self.post_process(item, db_id)

        except GeneratorExit:
            logger.info(
                f"Finished {self._descriptor} processing. Updated {self.updated}, added {self.added}, skipped {self.skipped}."
            )

    def post_process(self, item: TransformedBaseModel, db_id: PyObjectId) -> None:
        pass
