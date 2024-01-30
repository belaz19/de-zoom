--Homework week1
--Dmitriy Belozerov

--Q3
select count(1) from green_taxi_data where date_trunc('day', lpep_pickup_datetime) = '2019-09-18' and date_trunc('day', lpep_dropoff_datetime) = '2019-09-18' ; 

--15612


--Q4
select date_trunc('day', lpep_pickup_datetime) as date, sum(trip_distance) as td from green_taxi_data group by date order by td desc

--26 sep 2019


--Q5
select z."Borough", count (1) as rc
from green_taxi_data g
join zones z on g."PULocationID" = z."LocationID"
where date_trunc('day', g.lpep_pickup_datetime)='2019-09-18'
group by z."Borough"
order by rc desc
limit 3;

--Manhaaten, Brooklyn, Queens


--Q6
select g."DOLocationID", g.tip_amount from green_taxi_data g
join zones z on g."PULocationID" = z."LocationID"
where z."Zone" = 'Astoria' and date_trunc('month', g.lpep_pickup_datetime)='2019-09-01'
order by g.tip_amount desc
limit 5;

--zone 132 which is JFK airport