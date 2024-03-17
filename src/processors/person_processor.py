from functools import cache
from typing import Any, Dict

from models import Person
from nameparser import HumanName  # type: ignore

from .database_processor import DatabaseProcessor


class PersonProcessor(DatabaseProcessor[Person]):
    _descriptor = "person"
    _table_name = "people" 

    def _update_dictionary(self, person: Person) ->  dict:
        ratings: Dict[str, str] = {} # TODO: Get ratings  
        r = {"ratings": ratings} if ratings else {}
        return {"references": person.references} | r

    def _insert_dictionary(self, person: Person) -> dict:
        return HumanName(person.name).as_dict() | self._update_dictionary(person)

    @cache
    def find(self, person: Person) -> Any | None:
        found_person = self._table.find_one({"references": person.references})

        # if not found_person:
        #     name_parts = HumanName(person.name)       
        #     possibilities = self._table.find({"last": name_parts.last})
        #     for possibility in possibilities:
        #         if name_parts.first == possibility["first"] or (
        #             name_parts.first
        #             and possibility["first"]
        #             and name_parts.first[0] == possibility["first"][0]
        #             and name_parts.title == possibility["title"]
        #         ):
        #             found_person = possibility
        #             break
        
        return found_person

    def post_process(self, person: Person) -> None:
        pass
        # if person.race_id:
        #     client.handykapp.races.update_one(
        #         {"_id": person.race_id, "runners.horse": person.horse_id},
        #         {"$set": {f"runners.$.{person.role}": self.current_id}},
        #     )
