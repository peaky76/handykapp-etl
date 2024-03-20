from models import MongoPerson, Person
from nameparser import HumanName  # type: ignore

from .database_processor import DatabaseProcessor


class PersonProcessor(DatabaseProcessor[Person, MongoPerson]):
    _table_name = "people"
    _business_model = Person
    _db_model = MongoPerson 

    def find(self, person: MongoPerson) -> MongoPerson | None:
        found_person = self._table.find_one({"references": person.references}) if person.references else None

        if not found_person:    
            possibilities = self._table.find({"last": person.last})
            for possibility in possibilities:
                if person.first == possibility["first"] and person.middle == possibility["middle"]:
                # or (
                #     person.first
                #     and possibility["first"]
                #     and person.first[0] == possibility["first"][0]
                #     and person.title == possibility["title"]
                # ):
                    found_person = possibility
                    break
        
        return MongoPerson(**(found_person | {"db_id": found_person["_id"]})) if found_person else None

    def pre_process(self, person: Person) -> MongoPerson:
        return MongoPerson(**(person.model_dump(exclude={"name"}) | HumanName(person.name).as_dict()))
