from logging import Logger, LoggerAdapter

from clients import mongo_client as client
from models import ProcessPerson, PyObjectId
from nameparser import HumanName  # type: ignore

from .processor import Processor

db = client.handykapp


class PersonProcessor(Processor):
    _descriptor = "person"
    _next_processor = None

    def _update_dictionary(self, person):
        ratings = {} # TODO: Get ratings  
        return (
            {f"references.{person['source']}": person["name"]} | {"ratings": ratings}
            if ratings
            else {}
        )

    def _insert_dictionary(self, person):
        name_parts = HumanName(person["name"])
        ratings = {} # TODO: Get ratings

        return (
            name_parts.as_dict()
            | {f"references.{person['source']}": person["name"]}
            | {"ratings": ratings}
            if ratings
            else {}
        )

    def find(self, person: ProcessPerson) -> PyObjectId | None:
        found_person = db.people.find_one({"references": { person["source"]: person }}, {"_id": 1})

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
        
        return found_person["_id"]

    def update(self, person: ProcessPerson, db_id: PyObjectId) -> None:
        db.people.update_one(
            {"_id": db_id},
            {"$set": self._update_dictionary(person)},
        )
        

    def insert(self, person: ProcessPerson) -> PyObjectId:

        return db.people.insert_one(self._update_dictionary).inserted_id

    def post_process(self, person: ProcessPerson, db_id: PyObjectId, logger: Logger | LoggerAdapter) -> None:
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
