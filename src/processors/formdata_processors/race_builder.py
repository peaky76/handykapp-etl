from functools import cache, lru_cache
from itertools import combinations, pairwise
from typing import Literal, TypeAlias

from compytition import RankList
from horsetalk import RaceWeight
from prefect import get_run_logger

from models import FormdataRace, FormdataRunner
from processors import record_processor
from transformers.formdata_transformer import transform_races

RaceCompleteCheckResult: TypeAlias = dict[
    Literal["complete", "todo"], list[FormdataRunner]
]

failed_combos_memo = set()


@cache
def get_position_num(runner):
    pos = runner.position

    if "=" not in pos:
        return int(pos)

    return int(pos.split("p")[0].replace("=", ""))


def validate_positions(runners):
    try:
        RankList(r.position for r in runners)
        return True
    except ValueError:
        return False


def validate_ratings_vs_positions(finishers):
    adjusted_ratings = calculate_adjusted_ratings(
        tuple(runner.weight for runner in finishers),
        tuple(runner.allowance for runner in finishers),
        tuple(runner.form_rating for runner in finishers),
    )
    return is_monotonically_decreasing_or_equal(adjusted_ratings), adjusted_ratings


def is_monotonically_decreasing_or_equal(seq: list[float]) -> bool:
    return all(a >= b for a, b in zip(seq, seq[1:]))


@lru_cache(maxsize=128)
def calculate_adjusted_ratings(weights, allowances, form_ratings):
    """Cache rating calculations for performance"""
    return [
        rating - (RaceWeight(weight).lb + allowance)
        for weight, allowance, rating in zip(weights, allowances, form_ratings)
        if rating is not None
    ]


def build_record(race: FormdataRace, runners: list[FormdataRunner]) -> dict:
    return race.model_dump() | {"runners": [runner.model_dump() for runner in runners]}


def check_race_complete(
    race: FormdataRace, runners: list[FormdataRunner]
) -> RaceCompleteCheckResult:
    unchanged: RaceCompleteCheckResult = {"complete": [], "todo": runners}

    if len(runners) < race.number_of_runners:
        return unchanged

    # Sort runners first to reduce number of combinations
    is_finisher = lambda x: x.position.isdigit() or "=" in x.position
    finishers = sorted([r for r in runners if is_finisher(r)], key=get_position_num)

    # Fast path: If we have exact number of finishers, check if they form a valid race
    if len(finishers) == race.number_of_runners and validate_positions(finishers):
        ratings_valid, adjusted_ratings = validate_ratings_vs_positions(finishers)
        if not ratings_valid:
            return unchanged

        # Validate consistency of ratings vs beaten distances
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

        # Calculate pounds per length implied by each pair of horses
        ratios = [
            (b[0] - a[0]) / (b[1] - a[1]) if b[1] - a[1] != 0 else 0
            for a, b in pairwise(rtg_dist_pairs)
        ]

        # Validate consistency of pounds-per-length across the race
        non_zero_non_win_ratios = [r for r in ratios if r != 0][1:]
        if ratios and not all(
            abs(r1 - r2) <= 1 for r1, r2 in pairwise(non_zero_non_win_ratios)
        ):
            return {"complete": [], "todo": runners}

        # All validations passed - this is a complete race
        return {"complete": finishers, "todo": []}

    # Slow path: Check all possible combinations of runners
    for combo in combinations(runners, race.number_of_runners):
        combo_key = frozenset(id(r) for r in combo)
        if combo_key in failed_combos_memo:
            continue

        finishers = sorted([r for r in combo if is_finisher(r)], key=get_position_num)

        # Skip combinations with duplicate positions
        positions = [f.position for f in finishers]
        non_equal_positions = [p for p in positions if "=" not in p]
        if len(non_equal_positions) != len(set(non_equal_positions)):
            failed_combos_memo.add(combo_key)
            continue

        # Validate positions form a proper ranking
        if not validate_positions(finishers):
            failed_combos_memo.add(combo_key)
            continue

        # Validate ratings if all runners have them
        if all(runner.form_rating for runner in finishers):
            ratings_valid, adjusted_ratings = validate_ratings_vs_positions(finishers)
            if not ratings_valid:
                failed_combos_memo.add(combo_key)
                continue

            # Validate consistency of ratings vs beaten distances
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

            # Calculate pounds per length implied by each pair of horses
            ratios = [
                (b[0] - a[0]) / (b[1] - a[1]) if b[1] - a[1] != 0 else 0
                for a, b in pairwise(rtg_dist_pairs)
            ]

            # Validate consistency of pounds-per-length across the race
            non_zero_non_win_ratios = [r for r in ratios if r != 0][1:]
            if ratios and not all(
                abs(r1 - r2) <= 1 for r1, r2 in pairwise(non_zero_non_win_ratios)
            ):
                failed_combos_memo.add(combo_key)
                continue

        # Skip if non-finishers in this combo but unprocessed runners remain
        if len(finishers) != len(combo) and len(runners) != len(combo):
            failed_combos_memo.add(combo_key)
            continue

        # Found a valid combination
        return {
            "complete": list(combo),
            "todo": [r for r in runners if r not in combo],
        }

    # No valid combinations found
    return {"complete": [], "todo": runners}


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
                            transform_races,
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
                            transform_races,
                            "formdata",
                            "racing_research",
                        )
                    )
                    race_count += 1

                race_dict[race] = check_result["todo"]

                if len(race_dict[race]) == 0:
                    del race_dict[race]

    except GeneratorExit:
        logger.info(f"Built {race_count} races")
        for race, runners in race_dict.items():
            logger.info(f"\nRace: {race.course} {race.date}")
            logger.info(f"Runners ({len(runners)}/{race.number_of_runners}):")
            for runner in runners:
                logger.info(f"  {runner.name} ({runner.position})")
        r.close()
