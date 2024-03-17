from functools import cache
from typing import Any, ClassVar, Generic, Optional, Set, Type, TypeVar

from bson import ObjectId
from clients import mongo_client as client
from models import HashableBaseModel, MongoBaseModel
from peak_utility.listish import compact
from prefect import get_run_logger
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from .processor import Processor

T = TypeVar("T", bound=HashableBaseModel)
M = TypeVar("M", bound=MongoBaseModel)

class DatabaseProcessor(Processor[T], Generic[T, M]):
    _db_model: Type[M]
    _search_keys: ClassVar[Optional[Set[str]]] = None
    _update_keys: ClassVar[Optional[Set[str]]] = None
    _insert_keys: ClassVar[Optional[Set[str]]] = None
        
    def __init__(self, *, find_first: bool = True, prevent_update: bool = False):
        super().__init__()
        self.find_first: bool = find_first
        self.prevent_update: bool = prevent_update
        self.current_id: Optional[ObjectId] = None
        self.added: int = 0
        self.updated: int = 0
        self.unchanged: int = 0
        self.skipped: int = 0

    @property
    def _exit_message(self) -> str:
        return f"Finished {self._descriptor} processing. Updated {self.updated}, added {self.added}, skipped {self.skipped}, left {self.unchanged} unchanged."

    @property
    def _table(self) -> Collection:
        return client.handykapp[self._table_name]

    @property
    def _table_name(self) -> str:
        return f"{self._descriptor}s" 

    def _search_dictionary(self, item: T) -> dict: 
        return compact(item.model_dump(include=self._search_keys) if self._search_keys else item.model_dump())

    def _update_dictionary(self, item: T) -> dict:
        return compact(item.model_dump(include=self._update_keys) if self._update_keys else item.model_dump())

    def _insert_dictionary(self, item: T) -> dict:
        return compact(item.model_dump(include=self._insert_keys) if self._insert_keys else item.model_dump())

    @cache
    def find(self, item: T) -> M | None:
        db_item = self._table.find_one(self._search_dictionary(item))
        return self._db_model(**db_item) if db_item else None
        
    def update(self, item: T) -> None:
        self._table.update_one(
            {"_id": self.current_id},
            {"$set": self._update_dictionary(item)},
        )

    def insert(self, item: T) -> ObjectId:
        return self._table.insert_one(self._insert_dictionary(item)).inserted_id
        
    def process(self, item: T) -> None:
        logger = get_run_logger()

        def update_if_needed(item: T, db_item: Any):
            if not self.prevent_update:
                d = self._update_dictionary(item)
                if any(db_item[k] != d[k] for k in d):
                    self.update(item)
                    logger.debug(f"{item} updated")
                    self.updated += 1
                else:
                    logger.debug(f"{item} left unchanged")
                    self.unchanged += 1

        try:
            if self.find_first and (db_item := self.find(item)):
                self.current_id = ObjectId(db_item._id)
                update_if_needed(item, db_item)
            else:
                self.current_id = self.insert(item)
                logger.debug(f"{item} added to db")
                self.added += 1
        except DuplicateKeyError:
            if not self.find_first:
                db_item = self.find(item)
                if db_item:
                    self.current_id = ObjectId(db_item._id)
                    update_if_needed(item, db_item)
                else:
                    raise ValueError(f"Duplicate key error but no item found on db for {item}")
        except ValueError as e:
            logger.warning(e)
            self.skipped += 1
            
        total = self.updated + self.added + self.skipped + self.unchanged
        if total % 250 == 0:
            logger.info(f"Processed {total} {self._descriptor} records")

        self.post_process(item)   

    def post_process(self, item: T) -> None:
        pass
