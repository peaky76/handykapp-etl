from functools import cache
from typing import Any, ClassVar, List, Optional

from clients import mongo_client as client
from models import HashableBaseModel, PyObjectId
from peak_utility.listish import compact
from prefect import get_run_logger
from pydantic import BaseModel
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from .processor import Processor


class DatabaseProcessor(Processor):
    _search_keys: ClassVar[Optional[List[str]]] = None
    _update_keys: ClassVar[Optional[List[str]]] = None
    _insert_keys: ClassVar[Optional[List[str]]] = None
        
    def __init__(self, *, find_first: bool = True, prevent_update: bool = False):
        super().__init__()
        self.find_first = find_first
        self.prevent_update = prevent_update
        self.added = 0
        self.updated = 0
        self.unchanged = 0
        self.skipped = 0

    @property
    def _exit_message(self) -> str:
        return f"Finished {self._descriptor} processing. Updated {self.updated}, added {self.added}, skipped {self.skipped}, left {self.unchanged} unchanged."

    @property
    def _table(self) -> Collection:
        return client.handykapp[self._table_name]

    @property
    def _table_name(self) -> str:
        return f"{self._descriptor}s" 

    def _search_dictionary(self, item: HashableBaseModel) -> dict: 
        return compact(item.model_dump(include=self._search_keys) if self._search_keys else item.model_dump())

    def _update_dictionary(self, item: HashableBaseModel) -> dict:
        return compact(item.model_dump(include=self._update_keys) if self._update_keys else item.model_dump())

    def _insert_dictionary(self, item: HashableBaseModel) -> dict:
        return compact(item.model_dump(include=self._insert_keys) if self._insert_keys else item.model_dump())

    @cache
    def find(self, item: HashableBaseModel) -> HashableBaseModel | None:
        return self._table.find_one(self._search_dictionary(item))

    def update(self, item: HashableBaseModel, db_id: PyObjectId) -> None:
        self._table.update_one(
            {"_id": db_id},
            {"$set": self._update_dictionary(item)},
        )

    def insert(self, item: HashableBaseModel) -> PyObjectId:
        return self._table.insert_one(self._insert_dictionary(item))

    def process(self, item: HashableBaseModel):
        logger = get_run_logger()
        db_id = None

        def update_if_needed(item: HashableBaseModel, db_item: Any):
            if not self.prevent_update:
                d = self._update_dictionary(item)
                if any(db_item[k] != d[k] for k in d):
                    self.update(item, db_item["_id"])
                    logger.debug(f"{item} updated")
                    self.updated += 1
                else:
                    logger.debug(f"{item} left unchanged")
                    self.unchanged += 1

        try:
            if self.find_first and (db_item := self.find(item)):
                update_if_needed(item, db_item)
            else:
                db_id = self.insert(item)
                logger.debug(f"{item} added to db")
                self.added += 1
        except DuplicateKeyError:
            if not self.find_first:
                db_item = self.find(item)
                update_if_needed(item, db_item)
                db_id = db_item["_id"]
        except ValueError as e:
            logger.warning(e)
            self.skipped += 1
            
        total = self.updated + self.added + self.skipped + self.unchanged
        if total % 250 == 0:
            logger.info(f"Processed {total} {self._descriptor} records")

        self.post_process(item, db_id)   

    def post_process(self, item: HashableBaseModel, db_id: PyObjectId) -> None:
        pass
