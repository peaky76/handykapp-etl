from itertools import combinations
from typing import Literal, TypeAlias

from compytition import RankList
from horsetalk import RaceWeight
from prefect import get_run_logger

from models import FormdataRace, FormdataRunner
from processors import record_processor

RaceCompleteCheckResult: TypeAlias = dict[
    Literal["complete", "todo"], list[FormdataRunner]
]


def is_monotonically_decreasing_or_equal(seq: list[float]) -> bool:
    return all(a >= b for a, b in zip(seq, seq[1:]))


def check_race_complete(
    race: FormdataRace, runners: list[FormdataRunner]
) -> RaceCompleteCheckResult:
    unchanged = {"complete": [], "todo": runners}

    if len(runners) < race.number_of_runners:
        return unchanged

    is_finisher = lambda x: x.position.isdigit() or "=" in x.position

    for combo in combinations(runners, race.number_of_runners):
        try:
            finishers = sorted(
                [runner for runner in combo if is_finisher(runner)],
                key=lambda x: int(x.position.replace("=", "")),
            )
            RankList(runner.position for runner in finishers)
        except ValueError:
            continue

        adjusted_ratings = [
            runner.form_rating - (RaceWeight(runner.weight).lb + runner.allowance)
            for runner in finishers
        ]
        if not is_monotonically_decreasing_or_equal(adjusted_ratings):
            continue

        return {
            "complete": list(combo),
            "todo": [r for r in runners if r not in combo],
        }

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
