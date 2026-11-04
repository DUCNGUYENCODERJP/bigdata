// Bảng: analytics_summary (Parquet)
let
    Source = Parquet.Document(File.Contents("D:\Captone Project\4-bi\data\analytics_summary.parquet")),
    ChangeType = Table.TransformColumnTypes(Source,{{"PULocationID", Int64.Type}, {"pickup_hour", Int64.Type}, {"trip_count", Int64.Type}, {"avg_fare", type number}, {"avg_duration_min", type number}})
in
    ChangeType
