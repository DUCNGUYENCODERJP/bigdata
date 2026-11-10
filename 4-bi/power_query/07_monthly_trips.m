// Monthly trip demand from the full-history Spark aggregation.
let
    Source = full_history_workbook{[Item = "monthly_trips", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"month", type text}, {"trip_count", Int64.Type}}
    )
in
    ChangeType
