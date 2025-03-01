from itertools import combinations, pairwise
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


def build_record(race: FormdataRace, runners: list[FormdataRunner]) -> dict:
    return race.model_dump() | {"runners": [runner.model_dump() for runner in runners]}


def check_race_complete(
    race: FormdataRace, runners: list[FormdataRunner]
) -> RaceCompleteCheckResult:
    unchanged: RaceCompleteCheckResult = {"complete": [], "todo": runners}

    if len(runners) < race.number_of_runners:
        return unchanged

    is_finisher = lambda x: x.position.isdigit() or "=" in x.position

    for combo in combinations(runners, race.number_of_runners):
        finishers = sorted(
            [runner for runner in combo if is_finisher(runner)],
            key=lambda x: int(x.position.replace("=", "")),
        )

        # Guard for duplicate non-equal positions which would pass ranklist check
        positions = [f.position for f in finishers]
        non_equal_positions = [p for p in positions if "=" not in p]
        if len(non_equal_positions) != len(set(non_equal_positions)):
            continue

        # Check if this combo forms a proper ranking order
        try:
            RankList(runner.position for runner in finishers)
        except ValueError:
            continue

        # Check if the ratings of this combo would fit
        adjusted_ratings = [
            runner.form_rating - (RaceWeight(runner.weight).lb + runner.allowance)
            for runner in finishers
        ]
        if not is_monotonically_decreasing_or_equal(adjusted_ratings):
            continue

        # Check if the implied lbs per length of this combo would fit
        rtg_dist_pairs = [
            (r, d)
            for r, d in zip(
                adjusted_ratings,
                [
                    max(0, r.beaten_distance) if r.beaten_distance else None
                    for r in finishers
                ],
            )
            if d is not None
        ]
        ratios = [
            (b[0] - a[0]) / (b[1] - a[1]) if b[1] - a[1] != 0 else 0
            for a, b in pairwise(rtg_dist_pairs)
        ]

        non_zero_non_win_ratios = [r for r in ratios if r != 0][1:]
        if ratios and not all(
            abs(r1 - r2) <= 1 for r1, r2 in pairwise(non_zero_non_win_ratios)
        ):
            continue

        # Check if any non-finishers may possibly be from another race:
        if len(finishers) != len(combo) and len(runners) != len(combo):
            continue

        return {
            "complete": list(combo),
            "todo": [r for r in runners if r not in combo],
        }

    return unchanged


def race_builder():
    logger = get_run_logger()
    race_dict: dict[FormdataRace, list[FormdataRunner]] = {}
    race_count = 0

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

                check_result = check_race_complete(race, race_dict[race])

                if len(complete := check_result["complete"]):
                    record = build_record(race, complete)
                    r.send(
                        (
                            record,
                            lambda x: True,
                            lambda x: x,
                            "formdata",
                            "racing_research",
                        )
                    )
                    race_count += 1

                race_dict[race] = check_result["todo"]

                check_result = check_race_complete(race, race_dict[race])

                if len(complete := check_result["complete"]):
                    record = build_record(race, complete)
                    r.send(
                        (
                            record,
                            lambda x: True,
                            lambda x: x,
                            "formdata",
                            "racing_research",
                        )
                    )
                    race_count += 1

                if len(race_dict) == 0:
                    del race_dict[race]

    except GeneratorExit:
        logger.info(f"Built {race_count} races")
        for race, runners in race_dict.items():
            logger.info(f"\nRace: {race.course} {race.date}")
            logger.info(f"Runners ({len(runners)}/{race.number_of_runners}):")
            for runner in runners:
                logger.info(f"  {runner.name} ({runner.position})")
        r.close()
