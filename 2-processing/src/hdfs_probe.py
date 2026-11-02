"""Do kết nối WebHDFS của nhóm bằng một file mẫu, trước khi chuyển cả 33 GiB.

Máy nguồn không có `hdfs` CLI, nên dùng WebHDFS (HTTP REST) qua `requests` — thư viện
đã là dependency của dự án. Script đi hết vòng đời một file: tính sha256 tại nguồn →
MKDIRS → CREATE (lấy 307 từ NameNode) → PUT tới DataNode → OPEN đọc lại → đối chiếu
sha256. Nếu DataNode tự quảng bá địa chỉ container thì bước 307/PUT là nơi thất bại,
và đó chính là thứ cần phát hiện sớm trước khi đổ 1447 file.

Cách chạy (thiếu --src thì chỉ đo tới NameNode):

    .\\.venv\\Scripts\\python.exe src\\hdfs_probe.py --base http://IP:9870/webhdfs/v1 ^
        --src "data/clean/trips/year=2014/month=9/part-00000-de4269cc-ec9b-48b6-94bf-ee416623ccb9.c000.snappy.parquet" ^
        --dest /data/nyc_taxi/clean/_probe --user hadoop
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

CHUNK = 1024 * 1024
CLEAN_TOTAL_GIB = 33.34  # 1447 file Parquet của bộ Clean, dùng để suy ETA từ file mẫu

# Console Windows mặc định cp1252 sẽ ném UnicodeEncodeError khi log có dấu tiếng Việt.
sys.stdout.reconfigure(encoding="utf-8")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def webhdfs_url(base: str, hdfs_path: str, op: str, user: str | None, **params) -> str:
    query = [f"op={op}"]
    if user:
        query.append(f"user.name={quote(user)}")
    query.extend(f"{key}={quote(str(value))}" for key, value in params.items() if value is not None)
    return f"{base.rstrip('/')}{quote(hdfs_path)}?{'&'.join(query)}"


def fail(step: str, response: requests.Response) -> int:
    print(f"[FAIL] {step}: HTTP {response.status_code}")
    print(response.text[:2000])
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="WebHDFS probe cho một file Parquet.")
    parser.add_argument("--base", required=True, help="Ví dụ http://192.168.1.20:9870/webhdfs/v1")
    parser.add_argument("--src", help="File Parquet trên đĩa; thiếu thì chỉ đo NameNode.")
    parser.add_argument("--dest", default="/data/nyc_taxi/clean/_probe", help="Thư mục HDFS nhận file mẫu.")
    parser.add_argument("--user", help="user.name; cluster simple-auth thường vẫn cần tham số này.")
    parser.add_argument("--replication", type=int, default=1, help="Cluster một DataNode nên dùng 1.")
    parser.add_argument("--keep", action="store_true", help="Giữ file mẫu trên HDFS thay vì xoá khi PASS.")
    args = parser.parse_args()

    session = requests.Session()

    # Bước 0: NameNode có lắng nghe và cho đọc đường dẫn gốc không.
    response = session.get(webhdfs_url(args.base, "/", "GETFILESTATUS", args.user), timeout=15)
    print(f"[0/4] GETFILESTATUS / -> HTTP {response.status_code}  ({urlparse(args.base).netloc})")
    if response.status_code != 200:
        return fail("bước 0 - NameNode không phản hồi hoặc từ chối", response)
    if not args.src:
        print("[OK] Thông NameNode. Thêm --src để đo cả đường DataNode.")
        return 0

    local = Path(args.src)
    if not local.is_file():
        print(f"[FAIL] Không tìm thấy file nguồn: {local}")
        return 1
    size = local.stat().st_size
    source_sha = sha256_of(local)
    print(f"[src] {local.name}  {size / 1048576:.1f} MiB  sha256={source_sha}")

    print("[1/4] MKDIRS " + args.dest)
    response = session.put(
        webhdfs_url(args.base, args.dest, "MKDIRS", args.user, permission="755"), timeout=30
    )
    if response.status_code != 200:
        return fail("bước 1 - MKDIRS bị từ chối (thường do quyền thư mục /data)", response)

    remote = f"{args.dest.rstrip('/')}/{local.name}"
    print(f"[2/4] CREATE {remote}  replication={args.replication}")
    response = session.put(
        webhdfs_url(args.base, remote, "CREATE", args.user, overwrite="true",
                    replication=args.replication),
        allow_redirects=False,
        timeout=30,
    )
    if response.status_code != 307:
        return fail("bước 2 - NameNode không trả 307 tới DataNode", response)
    datanode = urlparse(response.headers["Location"])
    print(f"      307 -> DataNode {datanode.netloc}")
    if not datanode.hostname or datanode.hostname in ("localhost", "127.0.0.1"):
        print("[CẢNH BÁO] DataNode quảng bá loopback hoặc tên container; máy khác ghi không được.")

    started = time.monotonic()
    with local.open("rb") as handle:
        response = session.put(datanode.geturl(), data=handle, timeout=600)
    elapsed = time.monotonic() - started
    if response.status_code != 201:
        return fail("bước 3 - PUT tới DataNode thất bại", response)
    mib_per_s = size / 1048576 / max(elapsed, 1e-9)
    print(f"      PUT 201 trong {elapsed:.1f} s = {mib_per_s:.1f} MiB/s")

    print("[3/4] OPEN đọc lại và đối chiếu sha256")
    response = session.get(webhdfs_url(args.base, remote, "OPEN", args.user), stream=True, timeout=300)
    if response.status_code != 200:
        return fail("bước 4 - đọc lại thất bại", response)
    digest = hashlib.sha256()
    received = 0
    for chunk in response.iter_content(CHUNK):
        digest.update(chunk)
        received += len(chunk)
    response.close()
    print(f"      remote {received} B  sha256={digest.hexdigest()}")
    if received != size or digest.hexdigest() != source_sha:
        print("[FAIL] Dữ liệu không khớp sau vòng ghi–đọc")
        return 1

    print(f"[ETA] {mib_per_s:.1f} MiB/s => {CLEAN_TOTAL_GIB} GiB hết ~"
          f"{CLEAN_TOTAL_GIB * 1024 / mib_per_s / 60:.0f} phút")
    if args.keep:
        print("[4/4] PASS - giữ file mẫu theo --keep")
    else:
        session.delete(webhdfs_url(args.base, remote, "DELETE", args.user, recursive="false"), timeout=30)
        print(f"[4/4] PASS - đã xoá file mẫu {remote}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.exceptions.RequestException as error:
        # Port đóng hoặc firewall chặn là lỗi hay gặp nhất khi đo cluster của người khác.
        print(f"[FAIL] Không kết nối được WebHDFS: {error}")
        sys.exit(1)
