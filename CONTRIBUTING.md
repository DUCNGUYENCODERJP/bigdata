# Cách làm việc chung để không giẫm chân nhau

## 1. Ai sở hữu thư mục nào

| Đường dẫn | Chủ sở hữu — chỉ người này sửa |
|---|---|
| `1-warehouse/` | Người 1 |
| `2-processing/` | Người 2 |
| `3-analytics/` | Người 3 |
| `4-bi/` | Người 4 |
| `docs/`, `README.md`, `CONTRIBUTING.md`, `.github/` | bàn bạc trước, Người 2 giữ repo |

Sửa thư mục của người khác = **mở Issue trước**, được đồng ý mới làm. Đặc biệt nghiêm cấm tự ý đổi: schema 25 cột, `2-processing/config.yaml`, `2-processing/src/clean.py` (ngưỡng làm sạch) — đổi một trong ba thứ đó nghĩa là chạy lại 188 tháng.

## 2. Nhánh và Pull Request

- **Không push thẳng vào `main`** (đã bật branch protection).
- Tên nhánh: `<tên-ngắn>/<mục-dịch>`, ví dụ `duoc/hive-ingest`, `khoa/gbt-baseline`. Mỗi người chỉ tạo nhánh của chính mình — không dùng nhánh chung.
- Một PR = một việc. PR chỉ chạm vào thư mục của bạn thì merge ngay được; PR chạm thư mục khác cần chủ thư mục review (CODEOWNERS tự gán).
- Trước khi mở PR: `git pull --rebase origin main` rồi chạy test của slice bạn.

## 3. Cái gì không được commit

- **Dữ liệu**: mọi thứ trong `data/`, `*.parquet` (GitHub chặn 100 MB/file, repo này đã vượt 50 GB). Muốn chia dữ liệu: đưa lên HDFS/máy chủ nhóm, không đưa vào git.
- **`.venv/`**, `__pycache__/`, `.tmp/`, `spark-warehouse/`, `metastore_db/` — đã có trong `.gitignore`.
- **Khóa bí mật**: token, mật khẩu, cookie. Repo này **public**, mọi thứ bạn push đều công khai.
- Báo cáo sinh lại được thì cân nhắc; các file bằng chứng nghiệm thu của Người 2 đã được chọn có chủ đích trong `2-processing/.gitignore`.

## 4. Chạy thử slice của Người 2

```powershell
Set-Location 2-processing
python -m venv .venv
.\.venv\Scripts\pip.exe install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

Cần JDK 17; trên Windows cần thêm `winutils.exe` + `hadoop.dll` và `HADOOP_HOME`. Sửa các đường dẫn tuyệt đối trong `2-processing/config.yaml` theo máy của bạn.

## 5. Khi cần số liệu của nhau

- Số theo tháng, tỷ lệ loại, cờ chất lượng: `2-processing/reports/warehouse_summary.md` và `run_log.csv`.
- Định nghĩa từng cột và đơn vị: `2-processing/docs/schema.md`.
- Câu hỏi "dòng này có thật không / sao thiếu 2009-2010": xem `2-processing/docs/handover.md` mục 6.
