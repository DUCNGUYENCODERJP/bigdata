// Demand heatmap from the local workbook; Spark day-of-week 1 = Sunday.
let
    Source = full_history_workbook{[Item = "demand_heatmap", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"pickup_hour", Int64.Type}, {"day_of_week", Int64.Type}, {"trip_count", Int64.Type}}
    )
in
    ChangeType
