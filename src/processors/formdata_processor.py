from typing import ClassVar

from clients import mongo_client as client
from models import PyObjectId, TransformedFormdataEntry
from pymongo.collection import Collection

from .database_processor import DatabaseProcessor


class FormdataProcessor(DatabaseProcessor):
    _descriptor: ClassVar[str] = "formdata"
    _table: ClassVar[Collection] = client.handykapp.formdata
    _search_keys: ClassVar[list[str]] = ["name", "country", "year"]

    def update(self, entry: TransformedFormdataEntry, db_id: PyObjectId) -> None:
        runs = self._table.find_one({"_id": db_id})["runs"]

        for new_run in entry.runs:

            matched_run = next(
                (r for r in runs if r["date"] == new_run["date"]),
                None,
            )
            if matched_run:
                runs.remove(matched_run)
            runs.append(new_run)

        update_dict = {"runs": runs} | entry.model_dump(include=["prize_money", "trainer", "trainer_form"])
        self._table.update_one(
            {"_id": db_id},
            {"$set": update_dict},
        )
   
formdata_processor = FormdataProcessor().process
