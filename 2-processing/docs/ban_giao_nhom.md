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

---

## Cập nhật 2026-11-03 — kho chung đã chạy, câu hỏi gửi Người 1 ở trên không còn treo

Mảnh 1 (Hadoop + Hive trên Docker) đã dựng xong trên máy này và nghiệm thu bằng truy vấn thật. Chi tiết + lệnh chạy: [`1-warehouse/README.md`](../../1-warehouse/README.md); những chỗ đoán sai trước đây đã ghi rõ trong [`handover.md` mục 9](handover.md).

**Có sẵn trên HDFS:**

| Bảng | Nội dung | Nghiệm thu |
|---|---|---|
| `default.clean_nyc_taxi` | 33,33 GiB Parquet sạch, phân vùng `year=/month=` | 188/188 partition, `count(*) = 1.506.791.871` |
| `default.raw_nyc_taxi` | File gốc TLC chưa lọc, 19,55 GiB, 19 cột | 188/188 partition, `count(*) = 1.516.766.780` (scan thật qua đủ 188 file), **5.026.251 cuốc huỷ `fare_amount < 0`** |
| `default.clean_nyc_taxi_tlc` | **View** cùng dữ liệu, đổi tên cột về kiểu file gốc + khai lại kiểu thời gian | dành riêng cho code `3-analytics`; pruning vẫn cắt đúng 1 partition |

**Người 3** — code của bạn đang đòi cột `PULocationID` / `tpep_pickup_datetime` / `tpep_dropoff_datetime`, bảng sạch dùng tên chuẩn hoá, nên chạy qua view (không cần sửa code, `main.py` đã có cờ này). Lưu ý `analysis.py` của bạn tính thời lượng bằng `CAST(tpep_dropoff_datetime AS BIGINT)`: đọc **trực tiếp** `clean_nyc_taxi` sẽ lỗi `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` vì cột đó là `TIMESTAMP_NTZ`; view đã cast sẵn sang `TIMESTAMP` nên qua view thì chạy được. Lệnh chạy:

```powershell
spark-submit --conf spark.hadoop.hive.metastore.uris=thrift://<IP-máy-kho>:9083 `
  --conf spark.hadoop.fs.defaultFS=hdfs://<IP-máy-kho>:8020 `
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
  --conf spark.sql.catalogImplementation=hive `
  main.py --clean-table clean_nyc_taxi_tlc --raw-table raw_nyc_taxi
```

Cờ `dfs.client.use.datanode.hostname=true` **bắt buộc** khi Spark chạy ngoài mạng Docker — thiếu nó sẽ lỗi `could only be written to 0 of the 1 minReplication nodes`. `<IP-máy-kho>` là IP Wi-Fi của máy chạy cluster, xem `docker compose ps` ở máy đó; máy đổi mạng thì chạy `1-warehouse/tools/Set-Host.ps1` và xin lại IP.

**Đã chạy thử hộ trên partition 2024-01 bằng đúng code của bạn** (`analysis.py`, `feature_engineering.py`, `model.py`, `writer.py`…, không sửa dòng nào): PASS cả 6 nhóm hàm trong 521 s, ghi được Parquet/CSV lên HDFS. Một lưu ý để bạn khỏi mất một ngày: với **một tháng** + `maxIter=10` thì GBT **kém hơn baseline** (RMSE 48,0 so với 29,5; MAPE 923% so với 89%) — hãy chạy cả năm rồi mới kết luận về model.

**Người 4** — đọc cùng hai bảng qua đúng 4 dòng cấu hình trên, hoặc lấy Parquet Người 3 ghi ra ở `hdfs:///user/hive/warehouse/nyc_taxi_analytics/` (analytics_summary, fare_travel_by_hour, demand_heatmap, cancelled_trips, predictions, metrics).

**Còn treo:** metastore là Derby nhúng, **một người ghi một lúc**; `raw_nyc_taxi` thiếu `RatecodeID` và `passenger_count` (hai cột đổi kiểu INT64↔DOUBLE giữa các năm, không kiểu nào đọc được cả hai — đã đo), số dòng mới đối chiếu **tổng**, chưa so *từng tháng*; hai bảng này chạy trên máy cá nhân, không phải máy chủ chung của trường. `.venv` của mảnh 2 không có `numpy` nên `pyspark.ml` không chạy được bằng env đó — máy này đã có Python 3.11 (pyspark 4.0.1 + numpy) dùng được.

