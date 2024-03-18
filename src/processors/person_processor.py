from typing import Dict

from models import MongoPerson, Person
from nameparser import HumanName  # type: ignore

from .database_processor import DatabaseProcessor


class PersonProcessor(DatabaseProcessor[Person, MongoPerson]):
    _table_name = "people"
    _db_model = MongoPerson 

    def _update_dictionary(self, person: Person) -> dict:
        ratings: Dict[str, str] = {} # TODO: Get ratings  
        r = {"ratings": ratings} if ratings else {}
        return {"references": person.references} | r

    def _insert_dictionary(self, person: Person) -> dict:
        return HumanName(person.name).as_dict() | self._update_dictionary(person)


    def find(self, person: Person) -> MongoPerson | None:
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
        
        return MongoPerson(**(found_person | {"db_id": found_person["_id"]})) if found_person else None
