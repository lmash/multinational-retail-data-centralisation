from dataclasses import dataclass
from typing import List


@dataclass
class ColumnEntries:
    column_name: str
    entries: List


@dataclass
class ExtractCleanLoad:
    extracted_count: int
    clean_count: int


user_config = ExtractCleanLoad(extracted_count=15320, clean_count=15284)
card_config = ExtractCleanLoad(extracted_count=15309, clean_count=15284)
store_config = ExtractCleanLoad(extracted_count=451, clean_count=441)
product_config = ExtractCleanLoad(extracted_count=1853, clean_count=1846)
order_config = ExtractCleanLoad(extracted_count=120123, clean_count=120123)
date_times_config = ExtractCleanLoad(extracted_count=120161, clean_count=120123)
