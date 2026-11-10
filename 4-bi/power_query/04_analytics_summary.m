// Demand, fare, and duration by pickup zone and hour from the local workbook.
let
    Source = full_history_workbook{[Item = "analytics_summary", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"PULocationID", Int64.Type}, {"pickup_hour", Int64.Type}, {"trip_count", Int64.Type}, {"avg_fare", type number}, {"avg_duration_min", type number}}
    )
in
    ChangeType
