-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_taxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-belaz3/green_taxi_2022/*.parquet']
);

-- Check yellow trip data
SELECT * FROM ny_taxi.external_green_taxi_2022 limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_non_partitoned AS
SELECT * FROM ny_taxi.external_green_taxi_2022;

-- Question 1: count of records
select count(*) as row_count from ny_taxi.green_taxi_2022_non_partitoned;

-- Question 2: non-partitioned 6.41 MB, exteral 0 B
select count (distinct PULocationID) as count_PUL from ny_taxi.green_taxi_2022_non_partitoned;
select count (distinct PULocationID) as count_PUL from ny_taxi.external_green_taxi_2022;

-- Question 3: count when fare_amount = 0
select count(*) as count_fare_amount_zero from ny_taxi.green_taxi_2022_non_partitoned where fare_amount = 0;

-- Question 4: partition by lpep_pickup_datetime, cluster on PULocationID
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM ny_taxi.external_green_taxi_2022;

-- Question 5: distinct PULocationID between dates, non_partitioned 12.82 MB, Partitioned & clustered 1.12 MB
select distinct PULocationID as distinct_PUL from `ny_taxi.green_taxi_2022_non_partitoned`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
select distinct PULocationID as distinct_PUL from `ny_taxi.green_taxi_2022_partitoned_clustered`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';