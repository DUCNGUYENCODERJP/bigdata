// Hourly fare and trip duration from the local full-history Power BI workbook.
let
    Source = full_history_workbook{[Item = "fare_travel_by_hour", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"pickup_hour", Int64.Type}, {"trip_count", Int64.Type}, {"avg_fare", type number}, {"avg_duration_min", type number}}
    )
in
    ChangeType
