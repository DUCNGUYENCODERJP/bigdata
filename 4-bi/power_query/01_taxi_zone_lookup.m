// TLC taxi-zone dimension packaged with the full-history workbook.
let
    ZoneLookup = full_history_workbook{[Item = "taxi_zone_lookup", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        ZoneLookup,
        {
            {"LocationID", Int64.Type},
            {"Borough", type text},
            {"Zone", type text},
            {"service_zone", type text},
            {"MapLocation", type text}
        }
    )
in
    ChangeType
