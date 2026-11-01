"""Đường dẫn import dùng chung cho pytest.

Các module trong `src/` import lẫn nhau theo tên phẳng (`from clean import ...`) vì
pipeline chạy bằng `python src/pipeline.py`. Nếu chỉ thêm thư mục gốc vào `sys.path`
thì `src/output_validation.py` không tìm thấy `clean` khi pytest collection từng file
riêng lẻ, nên bổ sung cả `src/` tại đây cho mọi file test.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for entry in (str(ROOT / "src"), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)
