# Bàn giao — Người 2 · Xử lý dữ liệu

**Ngày:** 2026-10-29 · **Repo:** https://github.com/DUCNGUYENCODERJP/bigdata (branch `main`)

## Kết quả

188 tháng Yellow Taxi TLC **2011-01 → 2026-08** đã tải, chuẩn hoá, làm sạch bằng PySpark và ghi Parquet phân vùng `year=/month=`:

- **1.506.791.871 dòng**, 33,59 GiB, **188/188 partition**, **0 lệch schema**, **0 dòng vi phạm** sau kiểm tra
- Chỉ loại **0,658%** dòng; **34/34 test** xanh
- **25 cột**: 23 cột vật lý + `year`/`month` — khai báo đầy đủ ở `docs/schema.md`

## Giao những gì

| | |
|---|---|
| Code + test | `src/`, `tests/`, `config.yaml`, `scripts/run_batch.ps1` |
| Tài liệu | `docs/schema.md`, `docs/handover.md` (chi tiết kỹ thuật) |
| Bằng chứng | `reports/warehouse_summary.md`, `reports/run_log.csv`, `reports/source_manifest.csv` |
| Dữ liệu 33 GB | **vẫn ở máy Người 2**, chưa lên hệ thống chung — xem mục dưới |

## Mỗi người cần biết

**Người 1 — chốt giúp kênh ghi (việc duy nhất còn treo):**
1. Đích: bảng Hive `nyc_taxi.trips` hay một đường dẫn HDFS?
2. Metastore: shared Thrift hay embedded Derby? (Derby không dùng chung cả nhóm được)
3. Ai chạy lệnh ghi, trên máy nào?

Rẻ nhất: mình copy 33 GB lên HDFS, bạn chỉ việc đăng ký, **không cần ghi lại dữ liệu**:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi.trips USING parquet LOCATION 'hdfs://<nn>:8020/.../trips';
MSCK REPAIR TABLE nyc_taxi.trips;   -- nạp 188 partition từ tên thư mục
```

Lưu ý trung thực: nhánh `sink.type: hive` có trong code nhưng **chưa chạy thật, chưa có test**; chỉ đường dẫn `local` là đã nghiệm thu.

**Người 3 — hai điều ảnh hưởng mô hình:**
- 2025 có **5,9–9,4% dòng `fare_amount < 0`** (cuốc huỷ/refund) và **đã bị loại** → warehouse không còn fare âm nào; 2025 mất 8–11% dòng nên năm đó giới hạn loại là 12%, các năm khác 8%. Cần phân tích cuốc huỷ thì lấy từ raw.
- `passenger_count` **không dùng làm feature**. `2009–2010` không có trong warehouse (thiếu LocationID).

**Người 4:** `day_of_week` lấy **1 = Chủ nhật**; thời gian là giờ địa phương NYC, không kèm offset.

## Chạy lại từ đầu (nếu cần)

`.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force` — ~7,2 giờ, cần JDK 17 + 55 GB ổ trống, sửa 6 đường dẫn trong `config.yaml`. Gián đoạn thì chạy lại bỏ `-Force`.

**Đầu mối:** Người 2. Đổi cột/ngưỡng nào cũng phải chạy lại 188 tháng, nên hỏi trước khi tự đổi.
