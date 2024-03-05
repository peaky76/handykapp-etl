from typing import Optional

from prefect import get_run_logger


class Processor:
    _descriptor: str | None = None
    _next_processor: Optional["Processor"] = None
    _process_func: Optional[callable] = None   

    def process(self):
        logger = get_run_logger()
        logger.info(f"Starting {self._descriptor} processor")
                
        if self._next_processor:
            n = self._next_processor()
            next(n)
        else:
            n = None

        try:
            while True:
                item, source = yield
                added, updated, skipped = self._process_func(item, source, logger, n)
        except GeneratorExit:
            logger.info(
                f"Finished processing {self._descriptor}. Updated {updated}, added {added}, skipped {skipped}"
            )



# if (horse_id := horse_updater(horse, source)):
#         logger.debug(f"{name} updated")
#         updated_count += 1
# else:
#     try:
#         horse_id = horse_inserter(horse, source)
#         logger.debug(f"{name} added to db")
#         added_count += 1
#     except DuplicateKeyError:
#         logger.warning(
#             f"Duplicate horse: {name} ({horse.get('country')}) {horse.get('year')} ({horse['sex']})"
#         )
#         skipped_count += 1
#     except ValueError as e:
#         logger.warning(e)
#         skipped_count += 1


# if (person_id := person_updater(person, ratings, source)):
#     logger.debug(f"{person} updated")
#     updated_count += 1
# else:
#     try:
#         person_id = person_inserter(person, ratings, source)
#         logger.debug(f"{person} added to db")
#         added_count += 1
#     except DuplicateKeyError:
#         logger.warning(f"Duplicate person: {name}")
#         skipped_count += 1


# if (race_id := race_updater(racecourse_id, race, source)):
#     logger.debug(f"{race['datetime']} at {race['course']} updated")
#     updated_count += 1
# else:
#     try:
#         race_id = db.races.insert_one(
#             make_update_dictionary(race, racecourse_id)
#         ).inserted_id
#         logger.debug(
#             f"{race.get('datetime')} at {race.get('course')} added to db"
#         )
#         added_count += 1
#     except DuplicateKeyError:
#         logger.warning(
#             f"Duplicate race for {race['datetime']} at {race['course']}"
#         )
#         skipped_count += 1