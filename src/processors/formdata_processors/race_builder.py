from prefect import get_run_logger

from models import FormdataRace, FormdataRunner
from processors import record_processor


def race_builder():
    logger = get_run_logger()
    race_dict: dict[FormdataRace, list[FormdataRunner]] = {}

    r = record_processor()
    next(r)

    try:
        while True:
            horse = yield
            for run in horse.runs:
                race = FormdataRace(
                    **run.model_dump(
                        include={
                            "date",
                            "race_type",
                            "win_prize",
                            "course",
                            "number_of_runners",
                            "distance",
                            "going",
                        }
                    )
                )
                runner = FormdataRunner(
                    **run.model_dump(
                        include={
                            "weight",
                            "headgear",
                            "allowance",
                            "jockey",
                            "position",
                            "beaten_distance",
                            "time_rating",
                            "form_rating",
                        }
                    ),
                    **horse.model_dump(include={"name", "country", "year"}),
                )
                if race in race_dict:
                    race_dict[race].append(runner)
                else:
                    race_dict[race] = [runner]

    except GeneratorExit:
        # logger.info(
        #     f"Finished transforming {transform_count} races, rejected {reject_count}"
        # )
        r.close()
