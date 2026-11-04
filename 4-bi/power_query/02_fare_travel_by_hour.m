// Bảng: fare_travel_by_hour (Parquet)
let
    Source = Parquet.Document(File.Contents("D:\Captone Project\4-bi\data\fare_travel_by_hour.parquet")),
    ChangeType = Table.TransformColumnTypes(Source,{{"pickup_hour", Int64.Type}, {"trip_count", Int64.Type}, {"avg_fare", type number}, {"avg_duration_min", type number}})
in
    ChangeType
