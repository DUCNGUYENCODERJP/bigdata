"""Package full-history analytics exports into one Power BI-ready workbook."""

from __future__ import annotations
import logging

import argparse
import csv
import json
import os
import urllib.request
from pathlib import Path

from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo


ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TABLE_SHEETS = {
    "analytics_summary.csv": ("analytics_summary", "analytics_summary"),
    "fare_travel_by_hour.csv": ("fare_travel_by_hour", "fare_travel_by_hour"),
    "demand_heatmap.csv": ("demand_heatmap", "demand_heatmap"),
    "monthly_trips.csv": ("monthly_trips", "monthly_trips"),
    "zone_demand.csv": ("zone_demand", "zone_demand"),
    "predictions_zone_hour.csv": ("predictions", "predictions"),
    "evaluation_metrics.csv": ("metrics", "metrics"),
}


def _cell_value(value: str | None):
    if value is None or value == "":
        return None
    try:
        if "." not in value and "e" not in value.lower():
            return int(value)
        return float(value)
    except ValueError:
        return value


def _add_table_sheet(workbook: Workbook, name: str, table_name: str, headers, rows) -> None:
    sheet = workbook.create_sheet(name)
    sheet.append(list(headers))
    for row in rows:
        sheet.append([_cell_value(value) for value in row])
    if sheet.max_row < 2:
        raise ValueError(f"Table {name} has no data rows.")
    last_column = sheet.cell(row=1, column=sheet.max_column).column_letter
    table = Table(displayName=table_name, ref=f"A1:{last_column}{sheet.max_row}")
    table.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium2",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    sheet.add_table(table)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = table.ref
    sheet.sheet_view.showGridLines = False
    for column_cells in sheet.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells[:100])
        sheet.column_dimensions[column_cells[0].column_letter].width = min(max(max_length + 2, 12), 28)


def _read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        headers = next(reader, None)
        if not headers:
            raise ValueError(f"CSV has no header: {path}")
        rows = list(reader)
    return headers, rows


def _read_zone_lookup(source_path: Path) -> tuple[list[str], list[list[str]]]:
    if source_path.is_file():
        with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            return _zone_rows(reader)
    request = urllib.request.Request(ZONE_LOOKUP_URL, headers={"User-Agent": "NYC-Taxi-BI/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        reader = csv.DictReader(response.read().decode("utf-8-sig").splitlines())
        return _zone_rows(reader)


def _zone_rows(reader) -> tuple[list[str], list[list[str]]]:
    headers = ["LocationID", "Borough", "Zone", "service_zone", "MapLocation"]
    rows = []
    for row in reader:
        zone = row.get("Zone") or ""
        borough = row.get("Borough") or ""
        map_location = f"{zone}, {borough}, New York City, USA" if zone and borough else ""
        rows.append(
            [row.get("LocationID", ""), borough, zone, row.get("service_zone", ""), map_location]
        )
    return headers, rows


def package(artifact_dir: Path, output_path: Path) -> Path:
    manifest_path = artifact_dir / "evaluation_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    workbook = Workbook()
    workbook.remove(workbook.active)

    project_root = Path(__file__).resolve().parents[1]
    zone_source = project_root / "4-bi" / "power_bi_data" / "taxi_zone_lookup.csv"
    zone_headers, zone_rows = _read_zone_lookup(zone_source)
    _add_table_sheet(workbook, "taxi_zone_lookup", "taxi_zone_lookup", zone_headers, zone_rows)
    zone_by_id = {row[0]: row for row in zone_rows}
    for filename, (sheet_name, table_name) in TABLE_SHEETS.items():
        headers, rows = _read_csv(artifact_dir / filename)
        if filename == "zone_demand.csv":
            headers = [*headers, "Borough", "Zone", "MapLocation"]
            location_column = headers.index("PULocationID")
            rows = [
                [
                    *row,
                    *(zone_by_id.get(row[location_column], [None, None, None, None, None])[1:3]),
                    zone_by_id.get(row[location_column], [None, None, None, None, None])[4],
                ]
                for row in rows
            ]
        _add_table_sheet(workbook, sheet_name, table_name, headers, rows)

    reconciliation_files = list((project_root / "3-analytics" / "artifacts").glob("warehouse_reconciliation_*.csv"))
    if reconciliation_files:
        latest_reconciliation = max(reconciliation_files, key=lambda path: path.stat().st_mtime)
        headers, rows = _read_csv(latest_reconciliation)
        _add_table_sheet(workbook, "raw_reconciliation", "raw_reconciliation", headers, rows)

    manifest_sheet = workbook.create_sheet("run_manifest")
    manifest_sheet.append(["property", "value"])
    manifest_properties = [
        "created_at",
        "spark_version",
        "spark_master",
        "spark_driver_memory",
        "period_start",
        "period_end",
        "shared_partition_count",
        "included_trip_rows",
        "daily_zone_hour_rows",
        "distinct_dates",
        "train_date_count",
        "test_date_count",
        "train_ratio",
        "train_cutoff_date",
        "train_count",
        "test_count",
        "gbt_max_iter",
        "gbt_seed",
        "mape_rule",
    ]
    for key in manifest_properties:
        manifest_sheet.append([key, str(manifest.get(key, ""))])
    manifest_sheet.freeze_panes = "A2"
    manifest_sheet.column_dimensions["A"].width = 30
    manifest_sheet.column_dimensions["B"].width = 64
    manifest_sheet.sheet_view.showGridLines = False
    metadata_table = Table(displayName="run_manifest", ref=f"A1:B{manifest_sheet.max_row}")
    metadata_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    manifest_sheet.add_table(metadata_table)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(".tmp.xlsx")
    workbook.save(temporary_path)
    os.replace(temporary_path, output_path)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "4-bi" / "power_bi_data" / "NYC_Taxi_Full_History.xlsx",
    )
    args = parser.parse_args()
    result = package(args.artifact_dir.resolve(), args.output.resolve())
    logging.info(f"[POWER BI] Workbook saved: {result}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
