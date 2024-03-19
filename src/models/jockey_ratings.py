from typing import Dict

from .jockey_rating import JockeyRating
from .person import Person
from .year import Year


class JockeyRatings(Person):
    ratings: Dict[Year, JockeyRating]
