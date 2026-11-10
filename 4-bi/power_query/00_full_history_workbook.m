// Shared import source for the generated full-history Power BI workbook.
let
    Source = Excel.Workbook(
        File.Contents("D:\Captone Project\4-bi\power_bi_data\NYC_Taxi_Full_History.xlsx"),
        null,
        true
    )
in
    Source
