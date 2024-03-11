from clients import mongo_client as client
from models import PyObjectId, TransformedPerson
from nameparser import HumanName  # type: ignore
from pydantic import BaseModel

from .processor import Processor


class PersonProcessor(Processor):
    _descriptor = "person"
    _next_processor = None
    _table = client.handykapp.people

    def _update_dictionary(self, person) ->  dict:
        ratings = {} # TODO: Get ratings  
        return (
            {f"references.{person['source']}": person["name"]} | {"ratings": ratings}
            if ratings
            else {}
        )

    def _insert_dictionary(self, person) -> dict:
        name_parts = HumanName(person["name"])
        ratings = {} # TODO: Get ratings

        return (
            name_parts.as_dict()
            | {f"references.{person['source']}": person["name"]}
            | {"ratings": ratings}
            if ratings
            else {}
        )

    def find(self, person: TransformedPerson) -> BaseModel | None:
        found_person = self._table.find_one({"references": { person["source"]: person }}, {"_id": 1})

        if not found_person:
            name_parts = HumanName(person["name"])       
            possibilities = self._table.find({"last": name_parts.last})
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

    def post_process(self, person: TransformedPerson, db_id: PyObjectId) -> None:
        if person.race_id:
            client.handykapp.races.update_one(
                {"_id": person.race_id, "runners.horse": person.horse_id},
                {"$set": {f"runners.$.{person.role}": db_id}},
            )

person_processor = PersonProcessor().process
