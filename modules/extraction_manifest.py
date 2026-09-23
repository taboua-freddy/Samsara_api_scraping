"""Temporal coverage and stable identities for resumable Samsara extractions.

Intervals are half-open UTC milliseconds internally. Samsara's ``endMs`` is
inclusive, so callers send ``end_exclusive_ms - 1`` to the API.
"""

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone


def request_signature(table: str, endpoint: str, params: dict) -> str:
    """Hash only request parameters that change the data, not split settings."""
    static_params = {
        key: value
        for key, value in params.items()
        if key not in {"startMs", "endMs", "after", "endpoints"}
    }
    payload = json.dumps(
        {"table": table, "endpoint": endpoint, "params": static_params},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def partition_key(signature: str, start_ms: int, end_exclusive_ms: int) -> str:
    value = f"{signature}:{start_ms}:{end_exclusive_ms}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def partition_bounds(state: dict) -> tuple[int, int] | None:
    try:
        start = int(state["start_ms"])
        end = int(state["end_exclusive_ms"])
    except (KeyError, TypeError, ValueError):
        return None
    return (start, end) if start < end else None


def has_complete_coverage(
    start_ms: int,
    end_exclusive_ms: int,
    manifest: dict,
    signature: str,
    file_exists,
) -> bool:
    """True only when verified complete partitions cover every millisecond."""
    intervals = sorted(
        bounds
        for state in manifest.values()
        if isinstance(state, dict)
        and state.get("signature") == signature
        and state.get("status") == "complete"
        and (bounds := partition_bounds(state)) is not None
        and all(file_exists(path) for path in state.get("uploaded_files", []))
    )
    covered_to = start_ms
    for left, right in intervals:
        if right <= covered_to:
            continue
        if left > covered_to:
            return False
        covered_to = right
        if covered_to >= end_exclusive_ms:
            return True
    return covered_to >= end_exclusive_ms


def plan_timestamp_intervals(
    start_ms: int,
    end_exclusive_ms: int,
    window_ms: int,
    manifest: dict,
    signature: str,
    file_exists,
) -> list[tuple[int, int]]:
    """Return uncovered windows, preserving any partition with committed work."""
    if window_ms <= 0 or start_ms >= end_exclusive_ms:
        raise ValueError("Fenêtre temporelle invalide")
    boundaries = {start_ms, end_exclusive_ms}
    cursor = start_ms + window_ms
    while cursor < end_exclusive_ms:
        boundaries.add(cursor)
        cursor += window_ms

    completed = []
    resumable = []
    for state in manifest.values():
        if not isinstance(state, dict) or state.get("signature") != signature:
            continue
        bounds = partition_bounds(state)
        if bounds is None or bounds[1] <= start_ms or bounds[0] >= end_exclusive_ms:
            continue
        if state.get("status") == "in_progress" and (
            state.get("uploaded_files") or state.get("next_cursor")
        ):
            if bounds[0] < start_ms or bounds[1] > end_exclusive_ms:
                raise RuntimeError(
                    "Une partition partielle chevauche la plage demandée. "
                    "Relancer d'abord avec ses bornes d'origine."
                )
            resumable.append(bounds)
        elif state.get("status") == "complete" and all(
            file_exists(path) for path in state.get("uploaded_files", [])
        ):
            completed.append(bounds)
        elif state.get("status") != "split":
            # Existing failed/empty partitions may be re-tiled with this run's
            # window size, but their edges remain useful split points.
            pass
        boundaries.update(
            point for point in bounds if start_ms < point < end_exclusive_ms
        )
        if state.get("status") == "split":
            for child in state.get("children", []):
                if len(child) == 2:
                    boundaries.update(
                        point for point in child if start_ms < point < end_exclusive_ms
                    )

    if len(set(resumable)) != len(resumable):
        resumable = list(dict.fromkeys(resumable))
    for index, left in enumerate(resumable):
        if any(left[0] < right[1] and right[0] < left[1]
               for right in resumable[index + 1:]):
            raise RuntimeError("Partitions partielles chevauchantes dans le manifeste")

    protected = []
    for left, right in sorted([*completed, *resumable]):
        if protected and left <= protected[-1][1]:
            protected[-1] = (protected[-1][0], max(protected[-1][1], right))
        else:
            protected.append((left, right))
    points = sorted(boundaries)
    gaps = []
    protected_index = 0
    for left, right in zip(points, points[1:]):
        while (protected_index < len(protected)
               and protected[protected_index][1] <= left):
            protected_index += 1
        covered = (
            protected_index < len(protected)
            and protected[protected_index][0] <= left
            and right <= protected[protected_index][1]
        )
        if not covered:
            gaps.append((left, right))
    return sorted([*resumable, *gaps])


def legacy_utc_bounds(table: str, identity: str) -> tuple[int, int] | None:
    """Infer only exact legacy full-day or whole-second subday windows."""
    file_identity, separator, _endpoint = identity.partition("|")
    if not separator:
        return None
    name = file_identity.rsplit("/", 1)[-1]
    prefix = f"{table}_"
    if not name.startswith(prefix):
        return None
    date_part = name[len(prefix):]
    match = re.fullmatch(
        r"(\d{4}_\d{2}_\d{2})(?:_(\d{6})_to_(\d{4}_\d{2}_\d{2})_(\d{6}))?",
        date_part,
    )
    if not match:
        return None
    try:
        start_day = datetime.strptime(match.group(1), "%Y_%m_%d").replace(
            tzinfo=timezone.utc
        )
        if match.group(2):
            start = datetime.strptime(
                f"{match.group(1)}_{match.group(2)}", "%Y_%m_%d_%H%M%S"
            ).replace(tzinfo=timezone.utc)
            end = datetime.strptime(
                f"{match.group(3)}_{match.group(4)}", "%Y_%m_%d_%H%M%S"
            ).replace(tzinfo=timezone.utc) + timedelta(seconds=1)
        else:
            start, end = start_day, start_day + timedelta(days=1)
    except ValueError:
        return None
    if start >= end:
        return None
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)
