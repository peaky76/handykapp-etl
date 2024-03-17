from typing import ClassVar, Set

from models import FormdataEntry

from .database_processor import DatabaseProcessor


class FormdataProcessor(DatabaseProcessor[FormdataEntry]):
    _table_name = "formdata"
    _search_keys: ClassVar[Set[str]] = {"name", "country", "year"}

    def update(self, entry: FormdataEntry) -> None:
        existing_entry = self._table.find_one({"_id": self.current_id})
        existing_runs = existing_entry["runs"]
        incoming_runs = [r.model_dump() for r in entry.runs]
        merged_runs = {d["date"]: d for d in existing_runs + incoming_runs}

        update_dict = {"runs": list(merged_runs.values())} | entry.model_dump(include=["prize_money", "trainer", "trainer_form"])
        self._table.update_one(
            {"_id": self.current_id},
            {"$set": update_dict},
        )
