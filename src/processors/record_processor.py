import petl
from helpers import log_validation_problem
from prefect import get_run_logger
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
            record, validator, transformer, filename, source = yield
            data = petl.fromdicts([record])
            problems = validator(data)

            if len(problems.dicts()) > 0:
                logger.warning(f"Validation problems in {filename}")
                if len(problems.dicts()) > 10:
                    logger.warning("Too many problems to log")
                else:
                    for problem in problems.dicts():
                        log_validation_problem(problem)
                reject_count += 1
            else:
                try:
                    results = transformer(data)
                    for race in results:
                        r.send((race, source))
                    transform_count += 1
                
                except Exception as e:
                    logger.error(f"Error transforming {filename}: {e}")
                    reject_count += 1
                    continue

                if transform_count % 25 == 0:
                    logger.info(f"Read {transform_count} races. Current: {race['datetime']} at {race['course']}")

    except GeneratorExit:
        logger.info(
            f"Finished transforming {transform_count} races, rejected {reject_count}"
        )
        r.close()
