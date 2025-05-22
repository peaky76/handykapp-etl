import gc
from functools import cache, lru_cache
from itertools import combinations, pairwise
from typing import Literal, TypeAlias
from collections import Counter

from compytition import RankList
from horsetalk import RaceWeight
from prefect import get_run_logger

from models import FormdataRace, FormdataRecord, FormdataRunner
from processors import record_processor
from transformers.formdata_transformer import transform_races

RaceCompleteCheckResult: TypeAlias = dict[
    Literal["complete", "todo"], list[FormdataRunner]
]

failed_combos_by_race = {}


@cache
def get_position_num(runner):
    pos = runner.position

    if "=" not in pos:
        return int(pos)

    return int(pos.split("p")[0].replace("=", ""))


def all_duplicate_positions_have_equals(runners):
    """Check if all duplicate positions have equals in them"""
    # Get position numbers for all finishers
    position_nums = [get_position_num(r) for r in runners if is_finisher(r)]

    # Find position numbers that appear multiple times
    position_counts = Counter(position_nums)
    duplicate_positions = [pos for pos, count in position_counts.items() if count > 1]

    # If no duplicates, we're good
    if not duplicate_positions:
        return True

    # For each duplicate position number, check all matching runners have "=" in position
    for pos_num in duplicate_positions:
        matching_runners = [
            r for r in runners if is_finisher(r) and get_position_num(r) == pos_num
        ]
        if not all("=" in r.position for r in matching_runners):
            return False

    # All duplicates have "=" in their positions
    return True


def validate_positions(runners):
    try:
        RankList(r.position for r in runners)
        return all_duplicate_positions_have_equals(runners)

    except ValueError:
        return False


def validate_ratings_vs_positions(finishers):
    adjusted_ratings = calculate_adjusted_ratings(
        tuple(runner.weight for runner in finishers),
        tuple(runner.allowance for runner in finishers),
        tuple(runner.form_rating for runner in finishers),
    )
    return is_monotonically_decreasing_or_equal(adjusted_ratings), adjusted_ratings


def validate_ratings_vs_distances(finishers, adjusted_ratings):
    """Validate consistency between ratings and beaten distances"""
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

    # Skip validation if not enough pairs
    if len(rtg_dist_pairs) < 3:
        return True

    # Calculate ratios
    ratios = [
        (b[0] - a[0]) / (b[1] - a[1]) if b[1] - a[1] != 0 else 0
        for a, b in pairwise(rtg_dist_pairs)
    ]

    # Validate consistency
    non_zero_non_win_ratios = [r for r in ratios if r != 0][1:]
    return not ratios or all(
        abs(r1 - r2) <= 1 for r1, r2 in pairwise(non_zero_non_win_ratios)
    )


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


def build_record(race: FormdataRace, runners: list[FormdataRunner]) -> FormdataRecord:
    record = race.model_dump() | {
        "runners": [runner.model_dump() for runner in runners]
    }
    return FormdataRecord(**record)


def is_finisher(runner: FormdataRunner) -> bool:
    return runner.position.isdigit() or "=" in runner.position


def filtered_combinations(runners, race_number_of_runners):
    """Generate only potentially valid combinations based on race knowledge"""
    # Sort runners by position first (finishers with position "1" first)
    finishers = sorted([r for r in runners if is_finisher(r)], key=get_position_num)
    non_finishers = [r for r in runners if not is_finisher(r)]

    not_enough_runners = len(finishers) + len(non_finishers) < race_number_of_runners
    no_winner = get_position_num(finishers[0]) != 1 if finishers else True
    possibly_no_finishers = len(non_finishers) >= race_number_of_runners

    if not_enough_runners or (no_winner and not possibly_no_finishers):
        return []

    possible_combos = []

    if possibly_no_finishers:
        possible_combos = combinations(non_finishers, race_number_of_runners)

    possible_combos += [finishers[0]]

    for runner in finishers[1:]:
        combos_to_keep = []
        for combo in possible_combos:
            if len(combo) == race_number_of_runners:
                combos_to_keep.append(combo)
                continue

            last_runner = combo[-1]
            potential_new_combo = [*combo, runner]

            if get_position_num(runner) == get_position_num(last_runner):  # noqa: SIM102
                if "=" in runner.position and "=" in last_runner.position:
                    combos_to_keep.append(potential_new_combo)
                    continue

        if all(len(combo) == race_number_of_runners for combo in possible_combos):
            break

    return possible_combos


def check_race_complete(
    race: FormdataRace, runners: list[FormdataRunner]
) -> RaceCompleteCheckResult:
    unchanged: RaceCompleteCheckResult = {"complete": [], "todo": runners}

    if len(runners) < race.number_of_runners:
        return unchanged

    # Sort runners first to reduce number of combinations
    finishers = sorted([r for r in runners if is_finisher(r)], key=get_position_num)

    # Fast path: If we have exact number of finishers, check if they form a valid race
    if len(finishers) == race.number_of_runners and validate_positions(finishers):
        ratings_valid, adjusted_ratings = validate_ratings_vs_positions(finishers)
        if not ratings_valid:
            return unchanged

        if not validate_ratings_vs_distances(finishers, adjusted_ratings):
            return unchanged

        # All validations passed - this is a complete race
        return {"complete": finishers, "todo": []}

    # Get race hash for memo lookup
    race_hash = hash(race)

    # Initialize memo for this race if needed
    if race_hash not in failed_combos_by_race:
        failed_combos_by_race[race_hash] = set()

    # Get the memo for this specific race
    race_memo = failed_combos_by_race[race_hash]

    # # Slow path: Check all possible combinations of runners
    for combo in filtered_combinations(runners, race.number_of_runners):
        combo_key = frozenset(id(r) for r in combo)
        if combo_key in race_memo:
            continue

        finishers = sorted([r for r in combo if is_finisher(r)], key=get_position_num)

        # Skip combinations with duplicate positions
        positions = [f.position for f in finishers]
        non_equal_positions = [p for p in positions if "=" not in p]
        if len(non_equal_positions) != len(set(non_equal_positions)):
            race_memo.add(combo_key)
            continue

        # Validate positions form a proper ranking
        if not validate_positions(finishers):
            race_memo.add(combo_key)
            continue

        # Validate ratings if all runners have them
        if all(runner.form_rating for runner in finishers):
            ratings_valid, adjusted_ratings = validate_ratings_vs_positions(finishers)
            if not ratings_valid:
                race_memo.add(combo_key)
                continue

            if not validate_ratings_vs_distances(finishers, adjusted_ratings):
                race_memo.add(combo_key)
                continue

        # Skip if non-finishers in this combo but unprocessed runners remain
        if len(finishers) != len(combo) and len(runners) != len(combo):
            race_memo.add(combo_key)
            continue

        # Found a valid combination
        # Clear memo for this race since we don't need it anymore
        failed_combos_by_race.pop(race_hash, None)

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
                    if race_count and race_count % 100 == 0:
                        logger.info(
                            f"Memo sizes: {sum(len(memo) for memo in failed_combos_by_race.values())} combinations across {len(failed_combos_by_race)} races"
                        )
                        gc.collect()

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
                    if race_count and race_count % 100 == 0:
                        logger.info(
                            f"Memo sizes: {sum(len(memo) for memo in failed_combos_by_race.values())} combinations across {len(failed_combos_by_race)} races"
                        )
                        gc.collect()

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
