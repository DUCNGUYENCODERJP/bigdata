"""Do xem Spark doc duoc mot file Parquet bang kieu khong phat sinh tu file do khong.

Ly do can do: `raw_nyc_taxi` phai chju 188 thang Parquet khong cung schema —
`PULocationID` la INT64 o 145 thang dau/nhưng INT32 o 43 thang khac, `RatecodeID`
va `passenger_count` la INT64 o 133 thang roi DOUBLE o 55 thang. `mergeSchema` cua
Spark tu chop hop nhat INT64 voi INT32/DOUBLE (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`),
nen cach duy nhat la khai bao MOT kieu cho ca bang va do truc tiep: client doc file
"ngu" kieu co song duoc khong.

Thang doi chieu lay theo `2-processing/reports/source_manifest.csv` (manifest ghi
kieu vat ly that doc tu footer Parquet cua tung thang), KHONG do theo nam:

    pulocationid     INT64 -> 2011-01   INT32 -> 2024-01
    ratecodeid       INT64 -> 2024-01   DOUBLE -> 2018-07
    passenger_count  INT64 -> 2024-01   DOUBLE -> 2018-07
    airport_fee      INT32 -> 2011-02   DOUBLE -> 2024-01
    congestion_surcharge  INT32 -> 2019-06   DOUBLE -> 2011-01

Ket qua do ngay 2026-11-03 tren cluster nay, ca hai lan chay (log:
`2-processing/.tmp/probe_types_full.log` + `nullcheck2019.log`). Cot "File that" lay
theo manifest, khong lay theo nghi van:

| Khai bao trong DDL | File that | Ket luan |
|---|---|---|
| `bigint` | INT64 (2011-01, PULocationID) | OK — min 1, max 265 |
| `bigint` | INT32 (2024-01, PULocationID) | OK — max 265 |
| `int` | INT64 (2011-01, PULocationID) | **LOI** `SchemaColumnConvertNotSupportedException: column [PULocationID], physicalType INT64, logicalType int` |
| `double` | INT64 (2011-01, 2024-01, RatecodeID/passenger_count) | **LOI** `FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH` (SQLSTATE KD001) |
| `bigint` | DOUBLE (2018-07, RatecodeID) | **LOI** `SchemaColumnConvertNotSupportedException: physicalType DOUBLE, logicalType bigint` |
| `double` | DOUBLE (2018-07, RatecodeID) | OK — max 99.0 |
| `double` | INT32 (2019-06, congestion_surcharge) | OK **va doi ra gia tri that**: 6.940.813/6.971.560 dong khong null, min −2.5, max 2.75 |
| `double` | INT32 (2011-02, congestion_surcharge) | OK nhung **rong**: ca thang 14.202.809 dong deu NULL, nen "OK" nay chi chung Spark chap nhan ke hoach doc, khong chung minh bien doi gia tri |
| `string` | BYTE_ARRAY (2024-01, store_and_fwd_flag) | OK — 2.824.462 dong khong null, min "N", max "Y" |
| `timestamp` | INT64 logical TIMESTAMP (2024-01) | OK — 2003-01-01 05:59:39 .. 2024-02-01 07:01:15 |
| `airport_fee` (chu thuong) | cot `Airport_fee` (chu hoa) 2024-01 | OK — 2.824.462 dong khong null, max 1.75 |
| `Airport_fee` (hoa) / `airport_fee` (thuong) | cot `airport_fee` (chu thuong) 2019-06 | **KHONG DO DUOC BANG GIA TRI**: ca hai khai bao deu ra `khong_null = 0`, ma 0 la ket qua trung tinh — xay ra ca khi gop duoc mot cot toan NULL lan khi KHONG gop duoc (cot thieu bi lap NULL, case cuoi bang). Khong can chieu hoa-duoi: `raw_nyc_taxi` khai bao chu thuong nen chi chieu hoa-duoi-o-file-2023+ anh huong, va chieu do da co 2.824.462 gia tri that o dong tren. |

Toan bo 15 dong tren in ra boi chinh script nay, lan chay 17:37 ngay 2026-11-03
(log `2-processing/.tmp/probe_types_full2.log`); khong con o nao phai dien thuy.

Thu nhat dang ngo: o 2024-01, `store_and_fwd_flag` va `airport_fee` cung co
2.824.462 dong khong null. Do lai trong MOT lan read (`null_overlap_2024_01.py`) thi
140.162 dong null o CA HAI cot, 0 dong chi null mot cot — hai tap null trung het,
khong phai trung hop ngau nhien cung khong phai loi do.

Hai dieu rut ra cho ai query `raw_nyc_taxi`:
- Dung `IS NOT NULL` truoc khi group theo `store_and_fwd_flag`: o 2024-01 co 140.162
  dong null, khong phai mot cot luc nao cung day.
- Khong duoc noi 140.162 dong do da duoc mảnh 2 loc ra khoi bang sạch: tháng
  2024-01 chi loai 40.552 dong (raw 2.964.624 -> sạch 2.924.072,
  `reports/warehouse_summary.md`), nen it nhat 99.610 dong null-ca-hai-cot van du
  giu. Thuc te `store_and_fwd_flag` khong xuat hien trong bang sạch cung khong
  (`docs/schema.md`: 23 cot vat ly khong co cot nay) — vo hieu voi `clean_nyc_taxi`
  va `clean_nyc_taxi_tlc`, chi anh huong khi lam viec truc tiep tren raw.

Ba ket luan quyet dinh schema cua bang, do du 15 ca khai bao/kieu-file that:
1. `bigint` phuc vu INT32 + INT64; `double` phuc vu INT32 + DOUBLE; **khong kieu nao
   doc duoc ca INT64 lan DOUBLE** (`double<-INT64` va `bigint<-DOUBLE` deu chet), nen
   `RatecodeID` va `passenger_count` bi loai khoi `raw_nyc_taxi` — day la ly do duy
   nhat, khong phai thoi quen dat ten.
2. Spark khong phan biet hoa/thuong khi gop ten cot, nen chi can khai bao mot ten
   `airport_fee` la ca 2023+ (file ghi `Airport_fee`) van doc duoc.
3. Mot thang raw chi co 19 cot (2011-01 khong co `cbd_congestion_fee`, 2026 moi co
   `request_source`). Cot bang khai bao ma file khong co -> Spark lap NULL chu
   khong chet, do la ly do bang union doc duoc ca 15 nam. Cung vi vay mot con so
   `khong_null = 0` khong tự chứng minh da gop duoc cot: phai doc song song voi
   khai bao trung ten voi file (case (5)) moi ket luan duoc.

Chay:
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        tools/probe_type_promotion.py
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

# warehouse_host.py o ben thu muc src/, script nay o tools/: them src/ vao duong dan
# tim module de lenh chay duoc tu bat cu thu muc nao.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from warehouse_host import resolve_host  # noqa: E402

RAW = "/data/nyc_taxi/raw/trips"
# Duong dan partition theo thang da chon o tren; ten bien ghi theo kieu vat ly THAT
# cua file do (doc tu manifest), khong phai theo nam.
M2011_01 = f"{RAW}/year=2011/month=1"   # PULocationID INT64, RatecodeID INT64
M2011_02 = f"{RAW}/year=2011/month=2"   # airport_fee INT32 (ca thang toan NULL)
M2018_07 = f"{RAW}/year=2018/month=7"   # RatecodeID/passenger_count DOUBLE
M2019_06 = f"{RAW}/year=2019/month=6"   # congestion_surcharge INT32 CO gia tri
M2024_01 = f"{RAW}/year=2024/month=1"   # PULocationID INT32, airport_fee DOUBLE


def build_spark(host: str) -> SparkSession:
    hdfs_uri = f"hdfs://{host}:8020"
    spark = (
        SparkSession.builder.master("local[*]").appName("probe-type-promotion")
        .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIR", "D:/tmp/spark"))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.hadoop.home.dir", os.environ.get("HADOOP_HOME", "D:/hadoop"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def try_read(spark: SparkSession, label: str, path: str, schema: str, exprs: str) -> None:
    """Doc path voi schema ep truoc, in ket qua hoac loi that (khong bat gi)."""
    try:
        row = spark.read.schema(schema).parquet(path).selectExpr(*exprs.split("|")).collect()[0]
        print(f"[OK]   {label}: " + ",  ".join(f"{c} = {row[i]}" for i, c in enumerate(row.asDict())))
    except Exception as exc:  # do chinh la muc dich cua probe: phai nhin thay loi
        first = str(exc).splitlines()[0]
        print(f"[LOI]  {label}: {first[:300]}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None,
                        help="IP may chay cluster; thieu thi tu phat hien (warehouse_host.py).")
    args = parser.parse_args()
    args.host = resolve_host(args.host)
    spark = build_spark(args.host)

    # (1) INT64/INT32 co cung doc duoc bang bigint khong — day la kha nang duy nhat
    # giu duoc PULocationID/DOLocationID/VendorID trong bang union.
    try_read(spark, "bigint <- file INT64 (2011-01, PULocationID)", M2011_01,
             "PULocationID bigint", "count(*) n|min(PULocationID) lo|max(PULocationID) hi")
    try_read(spark, "bigint <- file INT32 (2024-01, PULocationID)", M2024_01,
             "PULocationID bigint", "count(*) n|min(PULocationID) lo|max(PULocationID) hi")

    # (2) Chieu nguoc lai: khai bao `int` thi doc file INT64 the nao. Probe nay tra
    # loi cho hoi do; schema cua bang dung `bigint` nen khong can biet truoc.
    try_read(spark, "int <- file INT64 (2011-01, PULocationID)", M2011_01,
             "PULocationID int", "count(*) n|max(PULocationID) hi")

    # (3) double vs INT64: hai cot duy nhat bi loai khoi bang, can do ca hai phia.
    try_read(spark, "double <- file INT64 (2024-01, RatecodeID)", M2024_01,
             "RatecodeID double", "count(*) n|max(RatecodeID) hi")
    try_read(spark, "double <- file DOUBLE (2018-07, RatecodeID)", M2018_07,
             "RatecodeID double", "count(*) n|max(RatecodeID) hi")
    try_read(spark, "bigint <- file DOUBLE (2018-07, RatecodeID)", M2018_07,
             "RatecodeID bigint", "count(*) n|max(RatecodeID) hi")
    try_read(spark, "double <- file INT64 (2024-01, passenger_count)", M2024_01,
             "passenger_count double", "count(*) n|max(passenger_count) hi")

    # (4) double doc INT32. Do Tren 2011-02 truoc, sau do 2019-06: 2011-02 in [OK]
    # nhung 14.202.809 dong deu NULL nen KHONG chung minh bien doi gia tri; 2019-06
    # (congestion $1, airport fee NULL) co 6.940.813 dong khong null, moi thuyet phuc.
    try_read(spark, "double <- file INT32 (2011-02, congestion_surcharge, toan NULL)", M2011_02,
             "congestion_surcharge double", "count(*) n|count(congestion_surcharge) khong_null|max(congestion_surcharge) hi")
    try_read(spark, "double <- file INT32 (2019-06, congestion_surcharge CO gia tri)", M2019_06,
             "congestion_surcharge double",
             "count(*) n|count(congestion_surcharge) khong_null|min(congestion_surcharge) lo|max(congestion_surcharge) hi")
    try_read(spark, "timestamp <- file INT64-timestamp (2024-01)", M2024_01,
             "tpep_pickup_datetime timestamp",
             "count(*) n|min(tpep_pickup_datetime) lo|max(tpep_pickup_datetime) hi")
    try_read(spark, "string <- file BYTE_ARRAY (2024-01, store_and_fwd_flag)", M2024_01,
             "store_and_fwd_flag string",
             "count(*) n|count(store_and_fwd_flag) khong_null|min(store_and_fwd_flag) lo|max(store_and_fwd_flag) hi")

    # (5) Hoa/thuong: bang chi khai bao `airport_fee`, file 2024+ ghi `Airport_fee`.
    try_read(spark, "airport_fee (thuong) <- file Airport_fee (hoa) 2024-01", M2024_01,
             "airport_fee double", "count(*) n|count(airport_fee) khong_null|max(airport_fee) hi")
    # Chieu nguoc (khai bao hoa, file ghi thuong) KHONG do duoc: moi thang thu ra deu
    # khong_null = 0, ma 0 trung tinh — xa ra ca khi gop duoc cot toan NULL lan khi
    # khong gop duoc (case (6)). Thu them 2019-06 cho day du hai chieu tren cung file.
    try_read(spark, "Airport_fee (hoa) <- file airport_fee (thuong) 2019-06", M2019_06,
             "Airport_fee double", "count(*) n|count(Airport_fee) khong_null|max(Airport_fee) hi")
    try_read(spark, "airport_fee (thuong) <- file airport_fee (thuong) 2019-06 (goc doi chieu)", M2019_06,
             "airport_fee double", "count(*) n|count(airport_fee) khong_null|max(airport_fee) hi")
    # (6) Cot khong ton tai o thang duoc doc: Spark lap NULL, khong chet — nho vay
    # ma bang union 19 cot doc duoc ca 2011 (chua co cbd_congestion_fee).
    try_read(spark, "cbd_congestion_fee <- file 2011-01 (khong co cot nay)", M2011_01,
             "cbd_congestion_fee double", "count(*) n|count(cbd_congestion_fee) khong_null")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
