"""Merge monthly query results into dashboard aggregates."""
from __future__ import annotations

import copy
import re
import time

from .warehouse import _format_size, _month_range, _month_sort_key, _partition_month


def _count_map(group: dict) -> dict[str, float]:
    labels = group.get("all_labels", group.get("labels", []))
    counts = group.get("all_counts", group.get("counts", []))
    return {
        str(label): float(count or 0)
        for label, count in zip(labels, counts)
        if label is not None
    }


def _hour_weights(heatmap: list[list[int]]) -> list[int]:
    return [
        sum(int(day[hour] or 0) for day in heatmap if hour < len(day))
        for hour in range(24)
    ]


def _merge_partition_delta(current: dict, delta: dict, partition_month: str) -> dict:
    """Merge one newly discovered month into cached dashboard aggregates."""
    merged = copy.deepcopy(current)
    old_kpis = merged["kpis"]
    new_kpis = delta["kpis"]
    old_count = int(old_kpis.get("total_records") or 0)
    new_count = int(new_kpis.get("total_records") or 0)
    total_count = old_count + new_count

    for average_key in ("avg_fare", "avg_duration"):
        old_average = float(old_kpis.get(average_key) or 0)
        new_average = float(new_kpis.get(average_key) or 0)
        old_sum = old_average * old_count
        new_sum = new_average * new_count
        old_kpis[average_key] = round((old_sum + new_sum) / total_count, 2) if total_count else 0
    old_kpis["total_records"] = total_count
    old_kpis["total_revenue"] = round(
        float(old_kpis.get("total_revenue") or 0) + float(new_kpis.get("total_revenue") or 0),
        2,
    )
    old_kpis["queried_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    old_monthly = merged["monthly"]
    month_counts = {
        label: count
        for label, count in zip(old_monthly.get("labels", []), old_monthly.get("counts", []))
    }
    for label, count in zip(delta["monthly"].get("labels", []), delta["monthly"].get("counts", [])):
        if count is not None:
            month_counts[label] = int(count)
    month_counts[partition_month] = int(new_count)
    known_labels = [label for label in month_counts if re.fullmatch(r"\d{4}-\d{2}", label)]
    labels = _month_range(min(known_labels, key=_month_sort_key), max(known_labels, key=_month_sort_key))
    counts = [month_counts.get(label) for label in labels]
    loaded_months = sum(count is not None for count in counts)
    old_monthly.update(
        labels=labels,
        counts=counts,
        loaded_months=loaded_months,
        expected_months=len(labels),
        missing_months=[label for label, count in zip(labels, counts) if count is None],
    )
    old_kpis.update(
        period=f"{labels[0]} → {labels[-1]}",
        months_loaded=loaded_months,
        months_expected=len(labels),
    )

    old_borough = _count_map(merged["borough"])
    for label, count in _count_map(delta["borough"]).items():
        old_borough[label] = old_borough.get(label, 0) + count
    borough_rows = sorted(old_borough.items(), key=lambda item: item[1], reverse=True)
    borough_total = sum(count for _, count in borough_rows)
    merged["borough"] = {
        "labels": [label for label, _ in borough_rows],
        "counts": [int(count) for _, count in borough_rows],
        "percentages": [round(count / borough_total * 100, 2) if borough_total else 0 for _, count in borough_rows],
    }

    old_zones = _count_map(merged["top_zones"])
    had_complete_zones = "all_counts" in merged["top_zones"]
    for label, count in _count_map(delta["top_zones"]).items():
        old_zones[label] = old_zones.get(label, 0) + count
    zone_rows = sorted(old_zones.items(), key=lambda item: item[1], reverse=True)
    merged["top_zones"] = {
        "labels": [label for label, _ in zone_rows[:8]],
        "counts": [int(count) for _, count in zone_rows[:8]],
        "all_labels": [label for label, _ in zone_rows],
        "all_counts": [int(count) for _, count in zone_rows],
        "incremental_candidates": (
            bool(merged["top_zones"].get("incremental_candidates"))
            or not had_complete_zones
        ),
    }

    old_heatmap = merged["heatmap"]
    new_heatmap = delta["heatmap"]
    combined_heatmap = [
        [
            int(old_heatmap[day][hour] or 0) + int(new_heatmap[day][hour] or 0)
            for hour in range(24)
        ]
        for day in range(7)
    ]
    old_hour_weights = _hour_weights(old_heatmap)
    new_hour_weights = _hour_weights(new_heatmap)
    old_hourly = merged["hourly"]
    new_hourly = delta["hourly"]
    old_hours = old_hourly.get("hours", [f"{hour}h" for hour in range(24)])
    new_hours = new_hourly.get("hours", [f"{hour}h" for hour in range(24)])
    old_hour_index = {hour: index for index, hour in enumerate(old_hours)}
    new_hour_index = {hour: index for index, hour in enumerate(new_hours)}
    combined_hourly = {"hours": [f"{hour}h" for hour in range(24)], "avg_fare": [], "avg_duration": []}
    for hour in range(24):
        key = f"{hour}h"
        old_index = old_hour_index.get(key)
        new_index = new_hour_index.get(key)
        old_weight = old_hour_weights[hour]
        new_weight = new_hour_weights[hour]
        for metric in ("avg_fare", "avg_duration"):
            old_value = float(old_hourly[metric][old_index] or 0) if old_index is not None else 0
            new_value = float(new_hourly[metric][new_index] or 0) if new_index is not None else 0
            weight = old_weight + new_weight
            combined_hourly[metric].append(
                round((old_value * old_weight + new_value * new_weight) / weight, 2) if weight else 0
            )
    merged["heatmap"] = combined_heatmap
    merged["hourly"] = combined_hourly

    old_cancelled = merged["cancelled"]
    new_cancelled = delta["cancelled"]
    total_raw = int(old_cancelled.get("total_raw") or 0) + int(new_cancelled.get("total_raw") or 0)
    total_cancelled = int(old_cancelled.get("total_cancelled") or 0) + int(new_cancelled.get("total_cancelled") or 0)
    old_cancelled["total_raw"] = total_raw
    old_cancelled["total_cancelled"] = total_cancelled
    old_cancelled["cancellation_rate"] = round(total_cancelled / total_raw * 100, 2) if total_raw else 0
    old_cancelled["hourly"] = [
        int(old_cancelled.get("hourly", [0] * 24)[hour] or 0)
        + int(new_cancelled.get("hourly", [0] * 24)[hour] or 0)
        for hour in range(24)
    ]
    historic_negative_total = old_cancelled.get("negative_fare_total")
    incremental_negative_total = new_cancelled.get("negative_fare_total")
    if historic_negative_total is not None and incremental_negative_total is not None:
        old_cancelled["negative_fare_total"] = round(
            float(historic_negative_total) + float(incremental_negative_total), 2
        )
    else:
        old_cancelled["negative_fare_total"] = None
    old_cancelled["refund_amount"] = None

    current_model_month = merged["ml"].get("model_partition", "")
    delta_model_month = delta["ml"].get("model_partition", "")
    if delta_model_month and (
        not current_model_month
        or _month_sort_key(partition_month) >= _month_sort_key(
            _partition_month(current_model_month) or "0000-00"
        )
    ):
        merged["ml"] = copy.deepcopy(delta["ml"])
    return merged


def _apply_storage_metadata(data: dict, warehouse_state: dict) -> None:
    storage_bytes = warehouse_state.get("storage_bytes")
    storage_files = warehouse_state.get("storage_files")
    if storage_bytes is None:
        return
    data["kpis"]["storage_bytes"] = storage_bytes
    data["kpis"]["file_count"] = storage_files
    file_suffix = f" ({storage_files:,} files)" if storage_files is not None else ""
    data["kpis"]["storage_size"] = f"{_format_size(storage_bytes)} Parquet{file_suffix}"
