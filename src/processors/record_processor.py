from functools import cache
from typing import Any, ClassVar, List

import petl  # type: ignore
from clients import mongo_client as client
from helpers import log_validation_problem
from prefect import get_run_logger

from processors.processor import Processor
from processors.race_processor import RaceProcessor

db = client.handykapp

@cache
def get_racecourse_id(course, surface, code, obstacle) -> str:
    surface_options = ["Tapeta", "Polytrack"] if surface == "AW" else ["Turf"]
    racecourse = db.racecourses.find_one(
        {
            "name": course.title(),
            "surface": {"$in": surface_options},
            "code": code,
            "obstacle": obstacle,
        },
        {"_id": 1},
    )
    return racecourse["_id"] if racecourse else None

class RecordProcessor(Processor):
    _forward_processors: ClassVar[List[Processor]] = [RaceProcessor()]

    def __init__(self):
        self.reject_count = 0
        self.transform_count = 0

    def process(self, item: Any):
        r = self.running_processors[0]
        logger = get_run_logger()
        record, validator, transformer, filename, source = item
        data = petl.fromdicts([record])
        problems = validator(data)

        if len(problems.dicts()) > 0:
            logger.warning(f"Validation problems in {filename}")
            if len(problems.dicts()) > 10:
                logger.warning("Too many problems to log")
            else:
                for problem in problems.dicts():
                    log_validation_problem(problem)
            self.reject_count += 1
        else:
            try:
                results = transformer(data)
            except Exception as e:
                logger.error(f"Error transforming {filename}: {e}")
                self.reject_count += 1
                return

            for race in results:
                racecourse_id = get_racecourse_id(
                    race["course"], race["surface"], race["code"], race["obstacle"]
                )
                creation_dict = race | { "racecourse_id": racecourse_id }
                r.send((creation_dict, source))
                self.transform_count += 1

            if self.transform_count % 25 == 0:
                logger.info(
                    f"Read {self.transform_count} races. Current: {race['datetime']} at {race['course']}"
                )
    
    @property
    def _exit_message(self):        
        return f"Finished transforming {self.transform_count} races, rejected {self.reject_count}"