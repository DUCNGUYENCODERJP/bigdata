// Bảng: metrics (CSV)
let
    Source = Csv.Document(File.Contents("D:\Captone Project\4-bi\data\metrics.csv"),[Delimiter=",", Columns=4, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromoteHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    ChangeType = Table.TransformColumnTypes(PromoteHeaders,{{"model", type text}, {"rmse", type number}, {"mae", type number}, {"mape", type number}})
in
    ChangeType
