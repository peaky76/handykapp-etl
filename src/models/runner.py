from typing import Optional

from .horse import Horse


class Runner(Horse):
    jockey: Optional[str] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    lbs_carried: Optional[int] = None 
    official_rating: Optional[int] = None