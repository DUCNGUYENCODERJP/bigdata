-- Đối soát số dòng theo tháng với source_manifest.csv.
-- Chạy bằng Spark SQL / Hive sau khi đăng ký hai bảng nguồn.
-- Kết quả: raw_rows, clean_rows, fare_negative_rows và excluded_other_rows.
-- Snapshot manifest 2011-01..2026-08 có raw_rows = 1,516,766,780.
-- Không đặt cận trên tháng: query bao gồm mọi partition mới được Hive đăng ký.

WITH raw_monthly AS (
    SELECT
        year,
        month,
        COUNT(*) AS raw_rows,
        SUM(CASE WHEN CAST(fare_amount AS DOUBLE) < 0 THEN 1 ELSE 0 END)
            AS fare_negative_rows
    FROM default.raw_nyc_taxi
    WHERE year >= 2011
    GROUP BY year, month
),
clean_monthly AS (
    SELECT
        year,
        month,
        COUNT(*) AS clean_rows
    FROM default.clean_nyc_taxi_tlc
    WHERE year >= 2011
    GROUP BY year, month
)
SELECT
    r.year,
    r.month,
    r.raw_rows,
    COALESCE(c.clean_rows, 0) AS clean_rows,
    r.fare_negative_rows,
    r.raw_rows - COALESCE(c.clean_rows, 0) - r.fare_negative_rows
        AS excluded_other_rows
FROM raw_monthly r
LEFT JOIN clean_monthly c
  ON r.year = c.year AND r.month = c.month
ORDER BY r.year, r.month;
