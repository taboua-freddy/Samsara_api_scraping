import json
from datetime import datetime
from pathlib import Path

import pytz

from .utils import cast_date, date_to_timestamp, timestamp_to_timestamp_ms

CATALOG_PATH = Path(__file__).resolve().parents[1] / "config" / "metadata_catalog.json"


def _render(value, variables: dict[str, str]):
    if isinstance(value, dict):
        return {key: _render(item, variables) for key, item in value.items()}
    if isinstance(value, list):
        return [_render(item, variables) for item in value]
    if isinstance(value, str):
        for key, replacement in variables.items():
            value = value.replace(f"${{{key}}}", replacement)
    return value


def load_metadata_catalog(start_time: str, end_time: str) -> list[dict]:
    with CATALOG_PATH.open(encoding="utf-8") as catalog_file:
        definitions = json.load(catalog_file)
    variables = {
        "START_RAW": start_time,
        "END_RAW": end_time,
        "START_DATE": cast_date(start_time),
        "END_DATE": cast_date(end_time),
        "START_MS": str(timestamp_to_timestamp_ms(date_to_timestamp(start_time))),
        "END_MS": str(timestamp_to_timestamp_ms(date_to_timestamp(end_time))),
        "START_ISO": datetime.strptime(start_time, "%d/%m/%Y")
        .replace(tzinfo=pytz.UTC)
        .isoformat(),
    }
    return _render(definitions, variables)
