# NYC TLC Yellow Taxi - Data Processing

Phần xử lý dùng PySpark local để kiểm tra, làm sạch Yellow Taxi Parquet, chuẩn hóa taxi zone và ghi Parquet phân vùng. Các đường dẫn raw, clean, reports và Spark local nằm trong `config.yaml` và là đường dẫn tuyệt đối mang tính máy — người nhận sửa 6 mục `paths:`/`hadoop_home` theo máy của mình. Bản bàn giao cho nhóm (kết quả nghiệm thu, cách tái tạo, lưu ý cho từng người) ở [docs/handover.md](docs/handover.md).

## Môi trường

- Python 3.9 trở lên
- Java 17 hoặc 21 (đã kiểm chứng với Java 17)
- PySpark 4.0.1, khai trong `requirements.txt`

Trong PowerShell, đặt JDK 17 trước khi chạy Spark (`scripts/run_batch.ps1` tự làm việc này, nhận `-JavaHome` và `-TempDir` để đổi đường dẫn):

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:TMP = "D:\tmp"
$env:TEMP = "D:\tmp"
```

Interpreter dùng cho dự án là Python của `.venv` tạo từ `requirements.txt`:

```powershell
.\.venv\Scripts\python.exe
```

Các thư viện được liệt kê trong `requirements.txt`. Không cần cài lại chúng nếu môi trường đã được chuẩn bị.

## Tải dữ liệu

Mặc định tải Yellow Taxi tháng `2024-01` cùng Taxi Zone Lookup. Downloader bỏ qua file đã có, lưu file đang tải với hậu tố `.part`, tiếp tục bằng HTTP Range và retry khi kết nối lỗi.

```powershell
.\.venv\Scripts\python.exe src\download.py --months 2024-01
```

Có thể truyền nhiều tháng bằng `--months 2024-01 2024-02`. Dữ liệu được lưu theo `paths.raw_dir` trong `config.yaml`.

Nguồn chính thức được liệt kê tại [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page); tệp được tải từ CloudFront của TLC.

## Kiểm tra chất lượng

```powershell
.\.venv\Scripts\python.exe src\quality_check.py --months 2024-01
```

Schema, null từng cột, bất thường và các mốc phân bố được in ra console và ghi vào `reports/quality_2024-01.md`.

## Chạy pipeline

```powershell
.\.venv\Scripts\python.exe src\pipeline.py --months 2024-01
```

Pipeline xóa duplicate hoàn toàn trước, sau đó áp dụng từng rule theo `cleaning.rule_order`. Log cho từng rule là số dòng bị loại sau khi các rule đứng trước đã chạy, nên không chồng lấp. Nếu tổng tỷ lệ loại vượt `cleaning.max_drop_rate` hoặc dữ liệu sau clean rỗng, pipeline dừng trước khi ghi output.

Sink mặc định `local` ghi Parquet vào `paths.clean_dir/trips`, phân vùng `year` và `month`; chạy lại sẽ overwrite các partition được ghi. Để chuyển sang Hive, đổi `sink.type` thành `hive` và cập nhật `sink.hive_table`. Khi chọn Hive, Spark bật `enableHiveSupport()` và ghi cùng schema/phân vùng bằng `saveAsTable`.

`passenger_count` được giữ cho thống kê mô tả; giá trị 0 được chuyển thành null. Không dùng cột này làm feature cho mô hình dự báo.

## Chạy batch nhiều tháng

`src\batch_runner.py` xử lý theo thứ tự tháng mới trước, tự bỏ qua tháng đã xong (postcheck partition trên đĩa), không dừng cả batch khi một tháng lỗi mà ghi `status=failed` vào `reports\run_log.csv`. Tháng có tỷ lệ loại vượt `cleaning.max_drop_rate` ghi `status=flagged` để người xem tự quyết, code không tự đổi ngưỡng. `--force` ghi lại cả partition đã xong; `--dry-run` chỉ ước lượng (không tải, không Spark).

```powershell
.\.venv\Scripts\python.exe src\batch_runner.py --from 2011-01 --to 2026-08 --force --dry-run
```

Wrapper PowerShell đặt JDK 17, vùng tạm, bật transcript và tắt ngủ khi cắm AC; đường dẫn dự án được suy ra từ vị trí script nên clone về máy nào cũng chạy:

```powershell
Set-Location <repo>\2-processing
.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force -KeepAwake
```

Khi gián đoạn, chạy lại cùng lệnh nhưng bỏ `-Force`: runner sẽ postcheck các partition đã có và chỉ xử lý tháng chưa xong. Exit code 1 nghĩa là có tháng `failed`; danh sách nằm ở cuối console và trong `reports\run_log.csv`.

## Tổng kết warehouse (mốc nghiệm thu)

```powershell
.\.venv\Scripts\python.exe src\warehouse_report.py --from 2011-01 --to 2026-08 --schema-doc docs\schema.md
```

Script mở một phiên Spark, đọc schema và số dòng của từng partition (đếm từ footer Parquet nên rẻ), so all partition với chuẩn `validation.schema_baseline_month`, đối chiếu `raw - removed = dòng trên đĩa` lấy số liệu từ `run_log.csv`, rồi ghi `reports/warehouse_summary.md` (kèm bảng 188 tháng, danh sách `failed`/`flagged`, partition thiếu hoặc lệch schema) và sinh lại `docs/schema.md` từ schema thật. Exit code 1 khi có partition lệch schema hoặc arithmetic không khớp; tháng chưa có `run_log` để trống chứ không đoán 0.

## Kiểm thử

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

`docs/schema.md` được sinh lại từ schema DataFrame thực tế khi pipeline chạy.