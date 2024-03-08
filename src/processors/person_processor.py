from logging import Logger, LoggerAdapter

from clients import mongo_client as client
from models import MongoPerson, ProcessPerson, PyObjectId
from nameparser import HumanName  # type: ignore

from .processor import Processor

db = client.handykapp


class PersonProcessor(Processor):
    _descriptor = "person"
    _next_processor = None

    def find(self, person: ProcessPerson, source: str) -> MongoPerson:
        found_person = db.people.find_one({"references": { source: person }})

        if not found_person:
            name_parts = HumanName(person["name"])       
            possibilities = db.people.find({"last": name_parts.last})
            for possibility in possibilities:
                if name_parts.first == possibility["first"] or (
                    name_parts.first
                    and possibility["first"]
                    and name_parts.first[0] == possibility["first"][0]
                    and name_parts.title == possibility["title"]
                ):
                    found_person = possibility
                    break
        
        return found_person

    def update(self, person: ProcessPerson, source: str) -> MongoPerson | None:
        if (found_person := self.find(person, source)):
            ratings = {} # TODO: Get ratings  
            update_data = (
                {f"references.{source}": person["name"]} | {"ratings": ratings}
                if ratings
                else {}
            )
            db.people.update_one(
                {"_id": found_person["_id"]},
                {"$set": update_data},
            )
            return found_person

        return None


    def insert(self, person: ProcessPerson, source: str) -> MongoPerson:
        name_parts = HumanName(person["name"])
        ratings = {} # TODO: Get ratings

        return db.people.insert_one(
            name_parts.as_dict()
            | {f"references.{source}": person["name"]}
            | {"ratings": ratings}
            if ratings
            else {}
        )

    def post_process(self, person: ProcessPerson, db_id: PyObjectId, source: str, logger: Logger | LoggerAdapter, next_processor: Processor) -> None:
        name = person["name"]
        race_id = person.get("race_id")
        horse_id = person.get("horse_id")
        role = person.get("role")

        # Add person to horse in race
        if race_id:
            db.races.update_one(
                {"_id": race_id, "runners.horse": horse_id},
                {"$set": {f"runners.$.{role}": db_id}},
            )

person_processor = PersonProcessor().process
