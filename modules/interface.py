from enum import Enum
from typing import Any, Literal


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


class ColumnToUpdate(str, Enum):
    DOWNLOAD = "last_download_time"
    TRANSFORMATION = "last_transformation_time"
    DATABASE = "last_db_migration_time"
    DATE_NO_DATA = "date_no_data"


class DownloadType(str, Enum):
    ONESHOT = "oneshot"
    TIME = "time"
