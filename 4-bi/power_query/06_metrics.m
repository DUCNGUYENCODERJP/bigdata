// RMSE, MAE, and MAPE from the saved full-history Spark ML evaluation.
let
    Source = full_history_workbook{[Item = "metrics", Kind = "Table"]}[Data],
    ChangeType = Table.TransformColumnTypes(
        Source,
        {{"model", type text}, {"rmse", type number}, {"mae", type number}, {"mape", type number}}
    )
in
    ChangeType
