from itertools import combinations
from typing import Literal, TypeAlias

from compytition import RankList
from prefect import get_run_logger

from models import FormdataRace, FormdataRunner
from processors import record_processor

RaceCompleteCheckResult: TypeAlias = dict[
    Literal["complete", "todo"], list[FormdataRunner]
]


def check_race_complete(
    race: FormdataRace, runners: list[FormdataRunner]
) -> RaceCompleteCheckResult:
    unchanged = {"complete": [], "todo": runners}
    n = race.number_of_runners

    if len(runners) < n:
        return unchanged

    is_finisher = lambda x: x.position.isdigit() or "=" in x.position

    for combo in combinations(runners, n):
        try:
            RankList(runner.position for runner in combo if is_finisher(runner))
            return {
                "complete": list(combo),
                "todo": [r for r in runners if r not in combo],
            }
        except ValueError:  # noqa: PERF203
            continue

    return unchanged


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
                if check_race_complete(race, race_dict[race]):
                    pass
                    # r.send()

    except GeneratorExit:
        # logger.info(
        #     f"Finished transforming {transform_count} races, rejected {reject_count}"
        # )
        r.close()
