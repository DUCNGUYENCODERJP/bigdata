// Bảng: predictions (Parquet)
let
    Source = Parquet.Document(File.Contents("D:\Captone Project\4-bi\data\predictions.parquet")),
    ChangeType = Table.TransformColumnTypes(Source,{{"date", type date}, {"PULocationID", Int64.Type}, {"pickup_hour", Int64.Type}, {"day_of_week", Int64.Type}, {"label", type number}, {"baseline_prediction", type number}, {"gbt_prediction", type number}})
in
    ChangeType
