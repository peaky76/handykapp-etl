from clients import mongo_client as client
from nameparser import HumanName  # type: ignore

from .processor import Processor

db = client.handykapp


class PersonProcessor(Processor):
    _descriptor = "person"
    _next_processor = None

    def update(self, person, source):
        found_person = db.people.find_one({"references": { source: person }})

        if not found_person:
            name_parts = HumanName(person["name"])
            ratings = {} # TODO: Get ratings         
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

        if found_person:
            person_id = found_person["_id"]
            update_data = (
                {f"references.{source}": person["name"]} | {"ratings": ratings}
                if ratings
                else {}
            )
            db.people.update_one(
                {"_id": person_id},
                {"$set": update_data},
            )
            return person_id

        return None


    def insert(self, person, source):
        name_parts = HumanName(person["name"])
        ratings = {} # TODO: Get ratings

        inserted_person = db.people.insert_one(
            name_parts.as_dict()
            | {f"references.{source}": person["name"]}
            | {"ratings": ratings}
            if ratings
            else {}
        )
        
        return inserted_person.inserted_id


    def post_process(self, person, person_id, source, logger, next_processor):
        name = person["name"]
        race_id = person.get("race_id")
        runner_id = person.get("runner_id")
        role = person.get("role")

        # Add person to horse in race
        if race_id:
            db.races.update_one(
                {"_id": race_id, "runners.horse": runner_id},
                {"$set": {f"runners.$.{role}": person_id}},
            )

person_processor = PersonProcessor().process
