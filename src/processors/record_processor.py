import petl  # type: ignore
from prefect import get_run_logger

from models import PreMongoRace
from processors.race_processor import race_processor


def record_processor():
    logger = get_run_logger()
    logger.info("Starting record processor")
    reject_count = 0
    transform_count = 0

    r = race_processor()
    next(r)

    try:
        while True:
            record, _validator, transformer, filename, source = yield
            data = petl.fromdicts([record])

            try:
                results = transformer(data)
            except Exception as e:
                logger.error(f"Error transforming {filename}: {e}")
                reject_count += 1
                continue

            for race in results:
                try:
                    validated_race = PreMongoRace(**race)
                    r.send((validated_race, source))
                    transform_count += 1
                except Exception as e:  # noqa: PERF203
                    logger.error(f"Validation error in {filename}: {e}")
                    reject_count += 1
                    continue

            if transform_count % 25 == 0:
                logger.info(
                    f"Read {transform_count} races. Current: {race['datetime']} at {race['course']}"
                )

    except GeneratorExit:
        logger.info(
            f"Finished transforming {transform_count} races, rejected {reject_count}"
        )
        r.close()
