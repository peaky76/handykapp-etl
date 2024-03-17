from typing import Optional

from .runner import Runner


class Run(Runner):
    position: Optional[int | str] = None
    distance_beaten: Optional[int | str] = None
    sp: Optional[str] = None