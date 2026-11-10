// Monthly HDFS raw Parquet footer reconciliation against the TLC source manifest.
let
    Source = full_history_workbook{[Item = "raw_reconciliation", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"month", type text}, {"manifest_rows", Int64.Type}, {"hive_raw_rows", Int64.Type}, {"delta_hive_minus_manifest", Int64.Type}, {"raw_parquet_files", Int64.Type}}
    )
in
    ChangeType
