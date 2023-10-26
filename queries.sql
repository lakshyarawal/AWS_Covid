SELECT * FROM "AwsDataCatalog"."covid-db"."cc_covid_input_data_lr"
WHERE new_cases > 0
AND new_deaths > 0;
