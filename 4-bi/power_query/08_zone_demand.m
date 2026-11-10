// Zone counts enriched with the official TLC zone name and map geocoding text.
let
    Source = full_history_workbook{[Item = "zone_demand", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"PULocationID", Int64.Type}, {"trip_count", Int64.Type}, {"Borough", type text}, {"Zone", type text}, {"MapLocation", type text}}
    )
in
    ChangeType
