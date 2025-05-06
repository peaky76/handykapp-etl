from pydantic import Field

from .formdata_race import FormdataRace
from .formdata_runner import FormdataRunner  # Assuming runners are of this type


class FormdataRecord(FormdataRace):
    runners: list[FormdataRunner] = Field(
        default_factory=list, description="List of runners in the race"
    )
