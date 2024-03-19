from typing import Dict

from models import MongoPerson, Person
from nameparser import HumanName  # type: ignore

from .database_processor import DatabaseProcessor


class PersonProcessor(DatabaseProcessor[Person, MongoPerson]):
    _table_name = "people"
    _business_model = Person
    _db_model = MongoPerson 

    def find(self, person: MongoPerson) -> MongoPerson | None:
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

    def pre_process(self, person: Person) -> MongoPerson:
        return MongoPerson(**(person.model_dump(exclude={"name"}) | HumanName(person.name).as_dict()))
