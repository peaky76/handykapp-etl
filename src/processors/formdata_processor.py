from typing import ClassVar, Set

from models import FormdataEntry, MongoFormdataEntry

from .database_processor import DatabaseProcessor


class FormdataProcessor(DatabaseProcessor[FormdataEntry, MongoFormdataEntry]):
    _table_name = "formdata"
    _search_keys: ClassVar[Set[str]] = {"name", "country", "year"}

    def update(self, entry: FormdataEntry) -> None:
        existing_entry = self._table.find_one({"_id": self.current_id})
        existing_runs = existing_entry["runs"] if existing_entry else []
        incoming_runs = [r.model_dump() for r in entry.runs]
        merged_runs = {d["date"]: d for d in existing_runs + incoming_runs}

        update_keys = {"prize_money", "trainer", "trainer_form"}
        update_dict = {"runs": list(merged_runs.values())} | entry.model_dump(include=update_keys)
        self._table.update_one(
            {"_id": self.current_id},
            {"$set": update_dict},
        )
