from typing import ClassVar

from models import FormdataEntry, PyObjectId

from .database_processor import DatabaseProcessor


class FormdataProcessor(DatabaseProcessor):
    _table_name = "formdata"
    _search_keys: ClassVar[list[str]] = ["name", "country", "year"]

    def update(self, entry: FormdataEntry, db_id: PyObjectId) -> None:
        existing_entry = self._table.find_one({"_id": db_id})
        existing_runs = existing_entry["runs"]
        incoming_runs = [r.model_dump() for r in entry.runs]
        merged_runs = {d["date"]: d for d in existing_runs + incoming_runs}

        update_dict = {"runs": list(merged_runs.values())} | entry.model_dump(include=["prize_money", "trainer", "trainer_form"])
        self._table.update_one(
            {"_id": db_id},
            {"$set": update_dict},
        )
