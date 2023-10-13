from dataclasses import dataclass
from typing import List


@dataclass
class ColumnEntries:
    column_name: str
    entries: List
