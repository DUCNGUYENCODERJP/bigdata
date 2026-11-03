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

Ket qua do ngay 2026-11-03 tren cluster nay (log: 2-processing/.tmp/probe_types.log).
Cot "File that" lay theo manifest, khong lay theo nghi van:

| Khai bao trong DDL | File that | Ket luan |
|---|---|---|
| `bigint` | INT64 (2011-01, PULocationID) | OK — min 1, max 265 |
| `bigint` | INT32 (2024-01, PULocationID) | OK — max 265 |
| `double` | INT64 (2011-01, RatecodeID) | **LOI** `FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH` (SQLSTATE KD001) |
| `double` | INT64 (2024-01, RatecodeID) | **LOI** nhu tren |
| `double` | INT64 (2024-01, passenger_count) | **LOI** nhu tren |
| `double` | DOUBLE (2011-01, congestion_surcharge) | OK |
| `string` | BYTE_ARRAY (2024-01, store_and_fwd_flag) | OK — max "Y" |
| `timestamp` | INT64 logical TIMESTAMP (2024-01) | OK — 2003-01-01 05:59:39 .. 2024-02-01 07:01:15 |
| `airport_fee` (chu thuong) | cot `Airport_fee` (chu hoa) 2024-01 | OK — 2.824.462 dong khong null, max 1.75 |

Lon nhat chua do (lan chay nay moi bo sung): `int <- INT64`, `bigint <- DOUBLE`,
`double <- INT32` (2011-02). Ba case nay khong anh huong schema cua `raw_nyc_taxi`
vi bang chi dung `bigint`/`double`/`timestamp`/`string`, nhung co do thi moi ket luan
duoc la "khong kieu nao dung cho moi cap".

Hai ket luan quyet dinh schema cua bang:
1. Khong co kieu nao doc duoc ca INT64 lan DOUBLE, nen `RatecodeID` va
   `passenger_count` bi loai khoi `raw_nyc_taxi`.
2. Spark khong phan biet hoa/thuong khi gop ten cot, nen chi can khai bao mot ten
   `airport_fee` la ca 2023+ (file ghi `Airport_fee`) van doc duoc.
3. Mot thang raw chi co 19 cot (2011-01 khong co `cbd_congestion_fee`, 2026 moi co
   `request_source`). Cot bang khai bao ma file khong co -> Spark lap NULL chu
   khong chet, do la ly do bang union doc duoc ca 15 nam.

Chay:
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        tools/probe_type_promotion.py --host 192.168.100.19
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

RAW = "/data/nyc_taxi/raw/trips"
# Duong dan partition theo thang da chon o tren; ten bien ghi theo kieu vat ly THAT
# cua file do (doc tu manifest), khong phai theo nam.
M2011_01 = f"{RAW}/year=2011/month=1"   # PULocationID INT64, RatecodeID INT64
M2011_02 = f"{RAW}/year=2011/month=2"   # airport_fee INT32
M2018_07 = f"{RAW}/year=2018/month=7"   # RatecodeID/passenger_count DOUBLE
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
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "192.168.100.19"))
    args = parser.parse_args()
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

    # (4) double doc INT32 (congestion_surcharge o 2011-02/2018-07 la INT32 that)
    # va timestamp doc INT64 logical.
    try_read(spark, "double <- file INT32 (2011-02, congestion_surcharge)", M2011_02,
             "congestion_surcharge double", "count(*) n|max(congestion_surcharge) hi")
    try_read(spark, "timestamp <- file INT64-timestamp (2024-01)", M2024_01,
             "tpep_pickup_datetime timestamp",
             "count(*) n|min(tpep_pickup_datetime) lo|max(tpep_pickup_datetime) hi")

    # (5) Hoa/thuong: bang chi khai bao `airport_fee`, file 2024+ ghi `Airport_fee`.
    try_read(spark, "airport_fee (thuong) <- file Airport_fee (hoa) 2024-01", M2024_01,
             "airport_fee double", "count(*) n|count(airport_fee) khong_null|max(airport_fee) hi")
    try_read(spark, "Airport_fee (hoa) <- file airport_fee (thuong) 2011-02", M2011_02,
             "Airport_fee double", "count(*) n|max(Airport_fee) hi")
    # (6) Cot khong ton tai o thang duoc doc: Spark lap NULL, khong chet — nho vay
    # ma bang union 19 cot doc duoc ca 2011 (chua co cbd_congestion_fee).
    try_read(spark, "cbd_congestion_fee <- file 2011-01 (khong co cot nay)", M2011_01,
             "cbd_congestion_fee double", "count(*) n|count(cbd_congestion_fee) khong_null")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
