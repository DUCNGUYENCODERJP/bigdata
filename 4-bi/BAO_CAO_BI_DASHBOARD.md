# 📑 BÁO CÁO NGHIỆM THU & TÍCH HỢP HỆ THỐNG BI DASHBOARD
## Đề Tài: Xây Dựng Nền Tảng Big Data Phân Tích & Dự Báo Nhu Cầu Taxi New York (NYC TLC Yellow Taxi)

> **Phân hệ:** Business Intelligence & Dashboard Integration  
> **Trạng thái:** Web dashboard đọc snapshot bền vững và tự bổ sung các partition Hive mới. Power BI Desktop đã được cài; report native đang được tạo từ các bảng export cục bộ để tránh phụ thuộc HiveServer2.
> **Mốc snapshot trong báo cáo:** 2026-11-05 19:35:00, giai đoạn 2011-01–2026-08.
> Các số liệu dưới đây là số liệu trong snapshot; chúng chỉ thay đổi sau khi cache nạp partition mới.

## 0. MỤC TIÊU, PHẠM VI VÀ NGUỒN SỐ LIỆU

Báo cáo mô tả dashboard BI cho dữ liệu NYC TLC Yellow Taxi: quy mô và xu hướng
chuyến đi, phân bố khu vực/khung giờ, cước và thời lượng, chất lượng dữ liệu,
cùng kết quả dự báo nhu cầu. Dashboard truy vấn Hive bằng Spark SQL; snapshot
JSON là lớp cache bền vững để khởi động nhanh và giữ phần tổng hợp đã đọc.

Phạm vi snapshot là 188 tháng từ 2011-01 đến 2026-08. Bảng sạch được dashboard
đọc qua view tương thích `default.clean_nyc_taxi_tlc`; dữ liệu gốc được đọc từ
`default.raw_nyc_taxi`. View sạch trỏ tới bảng vật lý `default.clean_nyc_taxi`.
Các số liệu trong báo cáo phản ánh snapshot nêu ở đầu trang, không phải số liệu
trực tiếp tại thời điểm đọc báo cáo nếu kho chưa có partition mới.

---

## 1. TỔNG QUAN TÍCH HỢP HỆ THỐNG ĐẦU-CUỐI (END-TO-END)

Hệ thống đã kết nối hoàn chỉnh qua 4 tầng kiến trúc Big Data phân tán:

```
┌────────────────────────────────────────────────────────────────────────┐
│                   KIẾN TRÚC TÍCH HỢP HỆ THỐNG (E2E)                    │
├────────────────────────────────────────────────────────────────────────┤
│ Tầng 1: Data Warehouse Layer                                           │
│   • Hadoop/HDFS và Hive service trên Docker: RPC 8020, WebHDFS 9870,   │
│     DataNode 9864/9866, Hive Metastore 9083                            │
│   • Clean: 33.33 GiB Parquet, 1447 file, 188 partition year/month      │
│   • View sạch: clean_nyc_taxi_tlc (schema default)                     │
│     → bảng vật lý default.clean_nyc_taxi                               │
│   • Bảng raw: default.raw_nyc_taxi                                     │
├────────────────────────────────────────────────────────────────────────┤
│ Tầng 2: Data Processing & ETL Pipeline Layer                           │
│   • PySpark ETL xử lý dữ liệu theo tháng (2011–2026)                   │
│   • ETL làm sạch, chuẩn hóa thời gian New York                         │
├────────────────────────────────────────────────────────────────────────┤
│ Tầng 3: Analytics & Machine Learning Layer                             │
│   • SQL tổng hợp nhu cầu, zone, giờ, heatmap, cước âm                  │
│   • Spark ML so sánh GBT/Baseline theo thời gian                       │
│   • Train/test theo thời gian, không rò rỉ tương lai                   │
├────────────────────────────────────────────────────────────────────────┤
│ Tầng 4: BI & Trực Quan Hóa Tương Tác Layer (Dashboard)                 │
│   • Dashboard tự nạp partition mới                                     │
│   • Data Model & Bộ công thức DAX Power BI: 4-bi/dax/                  │
│   • Scripts nạp dữ liệu Power Query: 4-bi/power_query/                 │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. KẾT QUẢ PHÂN TÍCH KINH DOANH & INSIGHTS (BUSINESS INSIGHTS)

### 2.0. Tóm tắt snapshot dữ liệu

| Chỉ số | Giá trị tại snapshot |
|---|---:|
| Giai đoạn / partition | 2011-01–2026-08 · 188/188 tháng |
| Chuyến sạch | 1,506,791,871 |
| Dòng dữ liệu gốc đã đối soát | 1,516,766,780 |
| Doanh thu cước | $26,971,594,498 |
| Cước bình quân | $17.90/chuyến |
| Thời lượng bình quân | 14.85 phút/chuyến |
| Clean Parquet trên HDFS | 33.33 GiB · 1,447 file |

**Kết quả đối soát:** footer Parquet của đủ 188 file raw trên HDFS cộng lại
1.516.766.780 dòng, khớp chính xác với `source_manifest.csv` (chênh lệch 0).
Cache cũ cao hơn 51.342 dòng; cache đã được hiệu chỉnh theo tổng đã xác minh.
Chi tiết theo tháng nằm ở `3-analytics/artifacts/warehouse_reconciliation_20261106.csv`.

### 2.1. Phân bố không gian & Thị phần các Quận (Boroughs & Top Zones)
- **Thị phần trong snapshot:** Manhattan chiếm **82.4%**, Queens **11.2%**, Brooklyn **5.1%**, Bronx **1.0%** và Staten Island **0.3%**.
- **Điểm nóng hàng đầu (Hotspots):**
  1. *Upper East Side South (Zone 237)*: Khu vực dân cư cao cấp có nhu cầu gọi xe thường trực.
  2. *Midtown Center & North (Zone 161, 163)*: Trung tâm tài chính, thương mại và văn phòng.
  3. *Times Square / Theatre District (Zone 230)*: Khu vui chơi giải trí và khách du lịch.
  4. *Sân bay JFK (Zone 132)*: Điểm phát sinh các chuyến đi giá trị cao nhất.

### 2.2. Nhịp sinh hoạt đô thị qua Ma trận nhiệt (24h $\times$ 7 ngày)
- Ma trận so sánh số chuyến theo **7 ngày trong tuần × 24 giờ**, sắp ngày từ Thứ Hai đến Chủ Nhật.
- Các ô được tô theo mức lưu lượng trong toàn ma trận; tooltip hiển thị số chuyến chính xác và tỷ lệ so với ô cao nhất.
- Insight về cao điểm là mô tả thống kê của snapshot; không suy ra nguyên nhân nhân quả từ biểu đồ.

### 2.3. Quy luật Giá cước & Thời gian di chuyển
- Trong snapshot, giá cước bình quân cao nhất khoảng **$22.50/chuyến lúc 4h**.
- Thời gian bình quân đạt đỉnh khoảng **24.2 phút lúc 16h**. Đây là tương quan theo giờ; dashboard không kết luận tắc đường là nguyên nhân duy nhất.

### 2.4. Phát hiện từ dữ liệu gốc (Cuốc cước âm / hoàn tiền)
- Cache hiện ghi **1.516.766.780 dòng raw đã đối soát** và **5.026.251 dòng fare âm (xấp xỉ 0,33%)**. Chênh lệch 51.342 chỉ thuộc snapshot cache cũ đã được sửa.
- Số dư ngoài nhóm clean và fare âm là **4.948.658** (`raw đã đối soát − sạch − fare âm`). Đây là phép trừ, không phải nhãn nguyên nhân ETL.
- Đối soát raw dùng row count ở footer Parquet theo tháng; không đọc lại nội dung 1,5 tỷ chuyến. Xem CSV/JSON/Markdown kết quả ở `3-analytics/artifacts/warehouse_reconciliation_20261106.*`.
- Tỷ lệ là tỷ trọng số bản ghi; không khẳng định mọi dòng fare âm đều là khoản hoàn tiền đã xác nhận.
- Nhóm cước âm/hoàn tiền được phân tích riêng, không tự gán nguyên nhân nếu chưa có bằng chứng.

---

## 3. ĐÁNH GIÁ MÔ HÌNH MACHINE LEARNING (GBT vs BASELINE)

Đánh giá toàn kỳ dùng 188 tháng từ `2011-01` đến `2026-08`, với split theo thời
gian 80/20: 13.679.753 dòng aggregate train và 3.772.274 dòng test.
`GBTRegressor(maxIter=50)` được đánh giá trên cùng tập test với baseline:

| Chỉ số đánh giá | Mô hình Cơ Sở (Baseline) | Mô hình GBTRegressor (Spark ML) | Mức độ cải thiện (%) |
|---|:---:|:---:|:---:|
| **RMSE (Sai số căn phương)** | 112.3053 | **42.2410** | 62.39% |
| **MAE (Sai số tuyệt đối)** | 57.6226 | **17.7288** | 69.23% |
| **MAPE (%) (Tỷ lệ sai số %)** | 306.0804% | **103.7799%** | 66.09% |

MAPE chỉ tính các dòng có nhu cầu thực tế lớn hơn 0. Artifact metrics và predictions
đã lưu ở `3-analytics/artifacts/full_history_20261106/`; dự báo trong CSV được
cộng gộp theo zone × giờ trên toàn kỳ test. Snapshot giao diện cũ và smoke test
2024-01 là kết quả partition/cấu hình khác, không trộn với kết quả toàn kỳ này.

---

## 4. KIẾN TRÚC CACHE VÀ TỰ ĐỒNG BỘ

Dashboard dùng Hive SQL làm nguồn dữ liệu; snapshot bền vững giúp khởi động nhanh
và tránh quét lại các tháng đã đọc. Snapshot được kiểm tra theo host, phạm vi,
tên bảng và cấu trúc dữ liệu. Tuổi snapshot không tự làm mất cache.

### 4.1. Luồng tự đồng bộ

1. Khi khởi động bằng `run_dashboard.ps1`, backend nạp snapshot hợp lệ vào RAM và
   đánh dấu các tháng có số liệu là đã đọc.
2. Worker nền gọi WebHDFS `GETCONTENTSUMMARY` cho cây clean và raw mỗi **5 giây**
   theo mặc định (`NYC_TAXI_CACHE_POLL_SECONDS`, tối thiểu 3 giây). Đây là truy vấn
   metadata, không đọc byte Parquet.
3. Khi chữ ký metadata thay đổi và ổn định qua **4 lần kiểm tra** (thường khoảng
   15 giây từ lần phát hiện đầu tiên), Spark cập nhật
   metadata partition Hive (`MSCK REPAIR TABLE`) rồi lấy danh sách tháng có ở cả
   bảng clean và raw. Ở lần nạp đầu, chính tập partition giao nhau xác định tháng
   bắt đầu cũ nhất và tháng kết thúc mới nhất; không còn cận cuối 2026-08 trong
   truy vấn `all`.
4. Backend truy vấn những tháng chưa có trong `covered_partitions`, gộp số liệu
   vào cache và ghi snapshot nguyên tử sau từng tháng. Nếu một tháng lỗi, các tháng
   đã lưu vẫn được giữ lại.
5. Trình duyệt kiểm tra `/api/status` mỗi 15 giây. Khi phiên bản cache đổi, biểu đồ
   tự tải lại âm thầm; không có nút đồng bộ thủ công hay thông báo thành công.

Luồng này xử lý dữ liệu theo đơn vị partition tháng của pipeline. Tháng đã đọc được
coi là bất biến: file bị thêm hoặc thay thế trong một tháng cũ không được đọc lại,
đúng với yêu cầu tránh quét trùng. Muốn cập nhật từng dòng trong partition đang mở,
cần nguồn append-only/CDC hoặc manifest delta để phân biệt bản ghi mới.

Nếu snapshot thiếu hoặc sai identity, lần nạp đầu phải dựng cache từ warehouse;
đây là trường hợp có thể cần quét toàn bộ phạm vi. Giữ file
`4-bi/cache/dashboard_hive_cache.json` để các lần chạy sau nạp từ cache.

### 4.2. Khởi động dashboard

Từ PowerShell tại thư mục gốc dự án:

```powershell
.\run_dashboard.ps1
```

Dashboard mở tại **`http://localhost:8080/dashboard.html`**. Cần JDK 17,
Hadoop Windows binaries, PySpark/NumPy và kết nối tới Hive Metastore 9083,
HDFS RPC 8020 cùng WebHDFS 9870. Dừng server bằng `Ctrl+C` tại cửa sổ chạy.

### 4.3. Các trang và biểu đồ

- **Tổng quan nhu cầu:** KPI chuyến/doanh thu, chuỗi tháng, thị phần quận, Top 8
  khu vực, heatmap 24 giờ × 7 ngày và insight.
- **Giá cước & chuyến hủy:** giá cước/thời lượng theo giờ, số chuyến cước âm/hủy
  theo giờ và biểu đồ phân loại clean/cước âm/ngoại lệ khác.
- **Dự báo học máy:** so sánh RMSE/MAE/MAPE, cấu hình train/test và chuỗi Actual,
  Baseline, GBT.
- **Xuất báo cáo:** trình duyệt hỗ trợ in/lưu PDF.

### 4.4. Power BI

Power BI Desktop đã được cài và mở thành công. HiveServer2 tại host kho mở TCP
nhưng đóng kết nối JDBC khi client gọi `OpenSession`; vì vậy report dùng workbook
cục bộ sinh từ các bảng Spark thay vì ODBC. Các query M/DAX và field wells cho
bản đồ zone, Matrix heatmap, cước/thời lượng và metrics ML nằm trong
`4-bi/power_query/`, `4-bi/dax/` và `4-bi/POWER_BI_BUILD_GUIDE.md`. `MapLocation`
hỗ trợ map theo vị trí đã geocode; choropleth polygon cần geometry TLC.

---

## 5. KIỂM TOÁN BIỂU ĐỒ VÀ GIỚI HẠN SỐ LIỆU

| Biểu đồ | Dữ liệu/đơn vị | Kiểm tra diễn giải |
|---|---|---|
| Sản lượng theo tháng | Số chuyến, 188 tháng | Tháng không có số liệu được phân biệt với tháng có 0 chuyến. |
| Thị phần borough | Số chuyến theo quận | Tỷ trọng dùng số đếm chưa làm tròn; phần trăm chỉ làm tròn ở nhãn. |
| Top 8 zone đón khách | Lượt đón theo zone | Sau khi gộp tháng mới, danh sách là ứng viên xấp xỉ, không phải tái xếp hạng toàn lịch sử. |
| Giá cước và thời lượng theo giờ | USD/chuyến và phút/chuyến | Hai đại lượng dùng hai trục riêng; đường trung bình không chứng minh quan hệ nhân quả. |
| Cước âm/hủy theo giờ | Số dòng raw có `fare_amount < 0` | Đếm theo giờ pickup hợp lệ; không tự suy luận nguyên nhân hủy. |
| Chất lượng dữ liệu | Sạch, cước âm/hủy, ngoại lệ khác | Ba nhóm cộng thành raw; nhóm ngoại lệ khác là phần dư tính toán. |
| Ma trận nhiệt | Số chuyến theo 7 ngày × 24 giờ | Màu chuẩn hóa theo ô cao nhất; tooltip giữ số chuyến. |
| Actual/Baseline/GBT | Số chuyến theo giờ trong partition mô hình | Hiển thị partition 2026-08; metric đánh giá dùng tập test theo thời gian. |

- Doughnut chất lượng dùng đúng ba nhóm **sạch**, **cước âm/hủy**, và **ngoại lệ
  khác**; tổng số lượng ba nhóm khớp tổng raw. Góc lát dựa trên số đếm thực.
- Cả hai doughnut dùng count chưa làm tròn làm tỷ trọng; nhãn phần trăm chỉ làm tròn
  khi hiển thị. Nếu một lát dương nhỏ hơn 2°, dashboard vẽ dấu cung cùng màu để
  nhận diện; chú giải và tooltip vẫn giữ số lượng/tỷ lệ thật. Số 0 vẫn là 0%, không
  bị nâng thành lát dương. Khi chưa có dữ liệu, doughnut dùng vòng trung tính.
- Snapshot ban đầu chỉ lưu Top 8 zone, không lưu toàn bộ phân bố lịch sử từng zone.
  Sau khi gộp tháng mới, Top zone được đánh dấu là danh sách ứng viên xấp xỉ và giao
  diện nêu rõ giới hạn này.
- Snapshot cũ không lưu tổng tiền fare âm chính xác cho toàn lịch sử; dashboard để
  trống chỉ số đó thay vì dùng ước lượng.

### Kiểm tra mã

Đã kiểm tra cú pháp Python, JavaScript, PowerShell và import backend. Ngày
2026-11-06, Spark metadata probe kết nối Hive thành công, xác nhận 188 partition
giao nhau từ 2011-01 đến 2026-08; HDFS báo 35.788.700.805 byte và 1.447 file
clean. Chưa quét dữ liệu chuyến đi toàn kỳ hoặc huấn luyện lại model trong lần
cập nhật này.

Kiểm tra cú pháp không xác nhận dữ liệu mới đã xuất hiện trong kho hoặc việc vẽ
trên trình duyệt. Sau khi cập nhật backend, khởi động lại `run_dashboard.ps1`;
trình duyệt tự nhận cache mới qua polling trạng thái.

---

## 6. KỊCH BẢN THUYẾT TRÌNH BẢO VỆ ĐỒ ÁN (5 PHÚT)

* **Phút 1 — Đặt vấn đề & Quy mô dữ liệu:**  
  *"Hệ thống giải quyết bài toán tối ưu hóa điều phối taxi New York trên quy mô Big Data thực sự: 188 tháng dữ liệu với hơn 1.5 tỷ bản ghi từ NYC TLC."*
* **Phút 2 — Hạ tầng & Xử lý (Data Warehouse & ETL):**  
  *"Kho dữ liệu HDFS lưu 33.33 GiB Parquet clean theo partition năm/tháng. Pipeline PySpark xử lý dữ liệu theo từng tháng."*
* **Phút 3 — Phân tích & Machine Learning:**  
  *"Spark SQL tạo phân tích zone × giờ và heatmap 24×7. Pipeline GBTRegressor chia train/test theo thời gian và tính RMSE, MAE, MAPE so với baseline. Kết quả phải được trình bày theo đúng partition/cấu hình của lần chạy; smoke test một tháng chưa chứng minh GBT tốt hơn."*
* **Phút 4 — Trực quan hóa:**
  *"Web dashboard hiện có các trang tổng quan, cước và dự báo. Power BI có nguồn Power Query/DAX và cấu hình map/heatmap; file report native cần refresh và nghiệm thu trong Power BI Desktop."*
* **Phút 5 — Kết luận:**  
  *"Dự án chứng minh tính khả thi của việc kết hợp kiến trúc phân tán Hadoop/Spark với trực quan hóa BI để giải quyết bài toán giao thông đô thị quy mô lớn."*

---

## 7. KẾT LUẬN VÀ ĐIỀU KIỆN NGHIỆM THU

Web dashboard có luồng truy vấn Hive SQL, snapshot cache bền vững, tự phát hiện
thay đổi metadata HDFS và bổ sung partition tháng chưa có trong cache. Giao diện
không còn thao tác đồng bộ thủ công hay toast báo nạp cache thành công. Raw count
đã được xác minh bằng footer của đủ 188 file Parquet: HDFS và manifest cùng có
1.516.766.780 dòng; cache cũ cao hơn 51.342 và đã được sửa. Đánh giá ML toàn kỳ
đã hoàn tất; workbook cục bộ đã đóng gói và có bảng metrics/predictions. Power BI
Desktop mở workbook connector nhưng bước Next không thể kích hoạt do nút ở trong
WebView2 con; hiện chưa có file `.pbix` và chưa kiểm chứng map/heatmap trực quan.

Đồng bộ cache chạy theo chu kỳ kiểm tra metadata và đơn vị tháng; HiveServer2
hiện đóng phiên JDBC nên phần nghiệm thu Power BI dùng nguồn workbook cục bộ.
