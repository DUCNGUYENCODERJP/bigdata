// Bảng: taxi_zone_lookup
let
    Source = Csv.Document(File.Contents("D:\Captone Project\4-bi\data\taxi_zone_lookup.csv"),[Delimiter=",", Columns=4, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromoteHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    ChangeType = Table.TransformColumnTypes(PromoteHeaders,{{"LocationID", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"service_zone", type text}})
in
    ChangeType
