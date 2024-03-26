from typing import Optional

from .entry import Entry


class Run(Entry):
    position: Optional[int | str] = None
    distance_beaten: Optional[int | str] = None
    sp: Optional[str] = None