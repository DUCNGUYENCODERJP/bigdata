// Bảng: demand_heatmap (Parquet)
let
    Source = Parquet.Document(File.Contents("D:\Captone Project\4-bi\data\demand_heatmap.parquet")),
    ChangeType = Table.TransformColumnTypes(Source,{{"pickup_hour", Int64.Type}, {"day_of_week", Int64.Type}, {"trip_count", Int64.Type}})
in
    ChangeType
