// Full-period held-out predictions aggregated by zone and hour.
let
    Source = full_history_workbook{[Item = "predictions", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"PULocationID", Int64.Type}, {"pickup_hour", Int64.Type}, {"actual_test_trips", Int64.Type}, {"baseline_prediction", type number}, {"gbt_prediction", type number}, {"test_slots", Int64.Type}}
    )
in
    ChangeType
