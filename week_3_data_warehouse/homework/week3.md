# Week 3 Homework
**Important Note:**

You can load the data however you would like, but keep the files in .GZ Format. If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

**NOTE:** You can use the CSV option for the GZ files when creating an External Table

**SETUP:**
Create an external table using the fhv 2019 data.
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table).
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv

* The data was uploaded to GSP (bucket) using the scriped ```parameterarized_flow.py```
* Via the UI it was transferred to BigQuery
* Create an external table:
```
CREATE OR REPLACE EXTERNAL TABLE `stoked-mode-375206.week3_homework_fhv.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoom/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
```
* Create a table in BG:
```
CREATE OR REPLACE TABLE `stoked-mode-375206.week3_homework_fhv.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `stoked-mode-375206.week3_homework_fhv.rides*`;
```

## Question 1:
What is the count for fhv vehicle records for year 2019?

65,623,481
43,244,696
22,978,333
13,942,414 

**Answer:** 43,244,696
```
SELECT count(*) FROM `stoked-mode-375206.week3_homework_fhv.rides*`;
```
