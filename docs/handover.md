# Bàn giao phần xử lý dữ liệu (Người 2)

Phạm vi: pipeline tải – kiểm tra – làm sạch – ghi warehouse cho **NYC TLC Yellow Taxi, 188 tháng từ 2011-01 đến 2026-08**.

## 1. Trạng thái nghiệm thu

| Chỉ tiêu | Giá trị | Bằng chứng |
|---|---|---|
| Partition trên đĩa | **188/188**, `year=/month=` | `reports/warehouse_summary.md` |
| Lệch schema giữa các partition | **0** | `docs/schema.md` (sinh từ schema thật) |
| Dòng trên đĩa | **1.506.791.871** | `reports/warehouse_summary.md` |
| Dòng bị loại | 9.974.909 = **0,657%** trên 1.516.766.780 dòng raw | `reports/run_log.csv` |
| Postcheck (`invalid`, `missing`) | **0 / 0** cho cả 188 tháng | `reports/run_log.csv`, `src/output_validation.py` |
| Kiểm thử | **34/34** xanh | `tests/` |
| Kích thước output | 36.068.314.549 B = 2894 file (1447 Parquet 35.788.700.805 B + 1447 `.crc` 279.613.744 B) | `reports/data_transfer_manifest.csv` |

Dữ liệu raw không nằm trong repo: **54 GB** (20 GB raw + 34 GB clean), trong raw có **101 file > 100 MB** — GitHub chặn cứng ở 100 MB/file và khuyến nghị repo < 1 GB. Đường lấy dữ liệu sạch: xem mục 4.

## 2. Những gì có trong repo

```
src/            pipeline: download.py, quality_check.py, clean.py, writer.py,
                output_validation.py, pipeline.py, batch_runner.py, warehouse_report.py
tests/          34 test (pytest), fixture tự dựng DataFrame, không cần mạng
config.yaml     đường dẫn, ngưỡng làm sạch, schema baseline, sink
scripts/        run_batch.ps1 — wrapper PowerShell cho batch dài
docs/           schema.md (23 cột vật lý + 2 cột phân vùng), handover.md
reports/        warehouse_summary.md, run_log.csv, source_manifest.csv
requirements.txt
```

Không lên GitHub (và không nên lên): `.venv/`, `data/`, `.tmp/`, transcript chạy batch (~900 KB), `reports/quality_summary.md` và `reports/quality_2024-01.md` là báo cáo mốc cũ, số đã bị `warehouse_summary.md` thay thế.

## 3. Bộ máy người nhận cần có

- **Python 3.9+** (môi trường đã kiểm chứng là 3.13) rồi tạo `.venv` riêng: `python -m venv .venv` và `pip install -r requirements.txt`. **PySpark 4.0.1 có trong `requirements.txt`, không cần cài thêm gì khác.**
- **JDK 17** — bắt buộc. JDK 21 trở lên (kể cả Java 25) không dùng được với Spark ở đây.
- **Hadoop binary cho Windows**: `winutils.exe` + `hadoop.dll` (Hadoop 3.4.0) và `HADOOP_HOME` trỏ tới thư mục `bin` chứa chúng.
- **~55 GB ổ trống** nếu tái tạo toàn bộ (20 GB raw + 34 GB clean), chưa tính vùng tạm Spark.

Đường dẫn máy-cụ-thể phải sửa: `config.yaml` có 6 mục `paths:`/`hadoop_home` dạng tuyệt đối (`D:/BIGDATA/2-processing/...`, `D:/tmp/spark`, `D:/hadoop`), và `JAVA_HOME` trong `scripts/run_batch.ps1` (đã tham số hoá: `-JavaHome`, `-TempDir`). `run_batch.ps1` tự suy ra thư mục dự án từ `$PSScriptRoot`, nên clone về đâu cũng chạy được.

## 4. Tái tạo warehouse từ số không

```powershell
Set-Location <thu-muc-repo>
.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force -KeepAwake
```

Runner tự tải thiếu → làm sạch → ghi Parquet → postcheck, theo thứ tự tháng mới trước, **không dừng cả batch khi một tháng lỗi** (ghi `status=failed` vào `reports/run_log.csv`). Gián đoạn thì chạy lại cùng lệnh bỏ `-Force`: tháng đã xong sẽ được postcheck và bỏ qua.

Chi phí đã đo trên máy nguồn: **~19,7 s cho mỗi 1 triệu dòng raw** (gồm tải + làm sạch + ghi + đọc lại kiểm tra), **toàn bộ 188 tháng mất ~7,2 giờ**. Chạy xong thì nghiệm thu bằng một lệnh:

```powershell
.\.venv\Scripts\python.exe src\warehouse_report.py --from 2011-01 --to 2026-08 --schema-doc docs\schema.md
```

Exit code 0 nghĩa là 188/188 partition, không lệch schema, `raw - removed` khớp số dòng trên đĩa.

## 5. Bàn giao dữ liệu 34 GB (kênh Google Drive / ổ cứng)

Copy **nguyên cây thư mục** `data/clean/trips` (2894 file, giữ nguyên tên `year=2024/month=1`). Không nén: Parquet đã Snappy, nên zip chỉ mất thời gian mà không giảm được bao nhiêu; nếu Drive làm hỏng tên thư mục có dấu `=`, tạo `.zip` ở chế độ *store* (không nén) thay vì nén lại.

Người nhận đối chiếu bằng manifest SHA-256 trong `reports/data_transfer_manifest.csv`:

```powershell
$root = "<duong-dan-data/clean/trips>"
$rows = Import-Csv "<duong-dan>\reports\data_transfer_manifest.csv"
foreach ($r in $rows) {
  $p = Join-Path $root ($r.relpath -replace '/', '\')
  if (-not (Test-Path $p)) { "THIEU: $($r.relpath)"; continue }
  if ((Get-Item $p).Length -ne [long]$r.size_bytes) { "LECH SIZE: $($r.relpath)" }
}
```

Muốn xác minh chặt hơn thì `Get-FileHash -Algorithm SHA256` từng file và so cột `sha256`; chỉ cần soi 1447 file `.parquet` vì `.crc` là sidecar của Hadoop, Spark tự bỏ qua.

## 6. Bốn điều phải nói thẳng với nhóm

1. **Người 1 (Data Warehouse):** output là **Parquet Hive-style trên đĩa local**, chưa phải Hive Metastore/HDFS. Hoặc trỏ thẳng `spark.read.parquet("<duong-dan>/data/clean/trips")`, hoặc đổi `sink.type: hive` (đã hỗ trợ sẵn trong `config.yaml` + `src/writer.py`) để pipeline tự `saveAsTable` — khi đó schema và phân vùng giữ nguyên.
2. **24 tháng 2009–2010 bị loại có chủ đích**: schema nguồn hai năm đó không có `PULocationID`/`DOLocationID`, mà thiết kế warehouse bắt buộc hai cột này. Ai cần hai năm đó phải quyết định riêng (geocode ngược hoặc bỏ cột zone).
3. **Người 3 (Analytics & ML) — quan trọng:** từ 2025-05 đến 2025-11 có cụm cuốc huỷ/refund với `fare_amount < 0` chiếm **6,3–9,4%** dòng, nên ngưỡng loại năm 2025 được nâng lên **12%** (`cleaning.max_drop_rate_by_year`) trong khi các năm khác vẫn 8%. Nếu pool 2025 với năm khác mà không biết điều này, số liệu lệch. Tháng 2025-12 trở lại 1,12% và 2026-08 là 0,42% — đây là thay đổi cách kê khai của nguồn, không phải lỗi tải.
4. **Người 3 / Người 4:** `passenger_count` chỉ dùng thống kê mô tả, **tuyệt đối không đưa vào feature** dự báo. Ba cờ chất lượng `is_short_trip`, `is_distance_missing`, `is_unknown_zone` nên được dùng làm bộ lọc khi dựng feature, không phải dữ liệu thiếu cần điền.

## 7. Liên hệ

Chủ sở hữu slice: Người 2 (data processing). Mọi câu hỏi về ngưỡng làm sạch, cột đầu ra hay phạm vi tháng đều do Người 2 quyết; thay đổi số dòng đã loại phải chạy lại `warehouse_report.py` để cập nhật `docs/schema.md`.
