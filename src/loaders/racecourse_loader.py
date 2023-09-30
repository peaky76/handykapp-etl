from prefect import task
from loaders.adders import add_racecourse

db = client.handykapp


@task
def load_racecourses(racecourses):
    for racecourse in racecourses:
        add_racecourse(racecourse)
