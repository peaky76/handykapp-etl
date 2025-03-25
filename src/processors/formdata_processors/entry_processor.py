from prefect import get_run_logger

from clients import mongo_client as client
from models.formdata_horse import FormdataHorse

db = client.handykapp


def entry_processor():
    logger = get_run_logger()
    logger.info("Starting entry processor")

    try:
        while True:
            horse = yield

            existing_entry = db.formdata.find_one(
                {
                    "name": horse.name,
                    "country": horse.country,
                    "year": horse.year,
                }
            )

            if existing_entry:
                existing_horse = FormdataHorse.model_validate(existing_entry)
                runs = existing_horse.runs

                for new_run in horse.runs:
                    matched_run = next(
                        (r for r in runs if r.date == new_run.date),
                        None,
                    )
                    if matched_run:
                        runs.remove(matched_run)
                    runs.append(new_run)

                db.formdata.find_one_and_update(
                    {
                        "name": horse.name,
                        "country": horse.country,
                        "year": horse.year,
                    },
                    {
                        "$set": {
                            "runs": [run.model_dump() for run in runs],
                            "prize_money": horse.prize_money,
                            "trainer": horse.trainer,
                            "trainer_form": horse.trainer_form,
                        }
                    },
                )
            else:
                db.formdata.insert_one(horse.model_dump())

    except GeneratorExit:
        pass
