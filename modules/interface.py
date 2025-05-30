from enum import Enum
from typing import Any


class TypeFilter(Enum):
    EXCEPTION_DATE = "exception_date"
    EXCEPTION_TABLE = "exception_table"
    DATE_RANGE = "date_range"
    VEHICLE_STATS = "vehicle_stats"
    DOWNLOADABLE_ONESHOT = "downloadable_oneshot"
    EXCEPTION = "exception"
    EXCEPTION_DATE_ONLY_START_DATE = "exception_date_only_start_date"
    ALL = "all"

class JSONNormalizeType(Enum):
    FAST_JSON_NORMALIZE = "fast_json_normalize_parallel"
    JSON_NORMALIZE = "json_normalize"


SplitDFConfig: type = dict[str, dict[str, Any]]


class SearchRetrieveType(Enum):
    FAMILY = "family"
    MAIN_FAMILY = "main_family"
    TABLE_NAME = "table_name"
    DATE_START = "start_date"
    DATE_END = "end_date"
    INDEX = "index"

