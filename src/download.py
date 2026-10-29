"""Tải Yellow Taxi và Taxi Zone Lookup từ nguồn NYC TLC."""

from __future__ import annotations

import argparse
import logging
import re
import time
from pathlib import Path
from typing import Any

import requests
import yaml

LOGGER = logging.getLogger("download")
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Đọc cấu hình YAML của dự án."""
    with config_path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def download_resumable(
    url: str,
    destination: Path,
    retries: int,
    timeout: tuple[int, int],
    chunk_size: int,
) -> None:
    """Tải theo từng khối, tiếp tục từ file .part nếu kết nối bị ngắt."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")

    if destination.exists() and destination.stat().st_size > 0:
        LOGGER.info("Bỏ qua file đã có: %s", destination)
        return

    for attempt in range(1, retries + 1):
        offset = partial.stat().st_size if partial.exists() else 0
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        received = 0
        try:
            with requests.get(
                url, headers=headers, stream=True, timeout=timeout
            ) as response:
                response.raise_for_status()
                append = offset > 0 and response.status_code == 206
                if offset and not append:
                    # Máy chủ bỏ qua Range: tải lại từ đầu để tránh nối sai dữ liệu.
                    offset = 0
                expected = response.headers.get("Content-Length")
                expected_bytes = int(expected) if expected is not None else None

                with partial.open("ab" if append else "wb") as output:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            output.write(chunk)
                            received += len(chunk)

                if expected_bytes is not None and received != expected_bytes:
                    raise IOError(
                        f"Tải chưa đủ byte: nhận {received}, cần {expected_bytes}"
                    )

            partial.replace(destination)
            LOGGER.info("Tải xong %s (%.2f MiB)", destination, destination.stat().st_size / 2**20)
            return
        except (requests.RequestException, OSError, ValueError) as error:
            LOGGER.warning(
                "Lần tải %d/%d lỗi cho %s: %s", attempt, retries, destination.name, error
            )
            if attempt == retries:
                raise
            time.sleep(min(2**(attempt - 1), 60))


def parse_months(values: list[str], configured: list[str]) -> list[str]:
    """Kiểm tra định dạng tháng và loại tháng lặp."""
    months = values or configured
    for month in months:
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
            raise ValueError(f"Tháng không hợp lệ: {month}; dùng YYYY-MM")
    return list(dict.fromkeys(months))


def main() -> None:
    parser = argparse.ArgumentParser(description="Tải dữ liệu NYC TLC Yellow Taxi")
    parser.add_argument("--months", nargs="+", help="Danh sách tháng YYYY-MM")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args.config)
    source = config["source"]
    raw_dir = Path(config["paths"]["raw_dir"])
    months = parse_months(args.months or [], config.get("months", []))
    timeout = (
        int(source["connect_timeout_seconds"]),
        int(source["read_timeout_seconds"]),
    )

    for month in months:
        filename = f"yellow_tripdata_{month}.parquet"
        download_resumable(
            source["trip_url_template"].format(month=month),
            raw_dir / filename,
            int(source["retries"]),
            timeout,
            int(source["chunk_size_bytes"]),
        )

    download_resumable(
        source["zones_url"],
        raw_dir / source["zones_filename"],
        int(source["retries"]),
        timeout,
        int(source["chunk_size_bytes"]),
    )


if __name__ == "__main__":
    main()