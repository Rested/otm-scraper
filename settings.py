from pathlib import Path
from typing import List, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    file_date: Optional[str] = None
    miles_from_london: int = 300
    property_types: List[str] = ("smallholding", "cottage", "house")
    max_connections: int = 10
    sleep_time: float = 0.2
    outputs_directory: Optional[Path] = "./outputs"
