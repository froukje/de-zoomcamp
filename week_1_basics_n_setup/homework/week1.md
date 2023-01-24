# Week 1 Homework

In this homework we'll prepare the environment and practice with Docker and SQL

## Question 1. Knowing docker tags

Run the command to get information on Docker

docker --help

Now run the command to get help on the "docker build" command

Which tag has the following text? - Write the image ID to the file

```--imageid string```

```--iidfile string```

```--idimage string```

```--idfile string```

**Answer:** ```--iidfile string```

## Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?

* 1
* 6
* 3
* 7

**Answer:** 3

|Package | Version|
|--------|--------|
|pip|        22.0.4|
|setuptools| 58.1.0|
|wheel|      0.38.4|

## Prepare Postgres

Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

* Start postgres is Docker:
```
docker run -it \
           -e POSTGRES_USER="root" \
           -e POSTGRES_PASSWORD="root" \
           -e POSTGRES_DB="ny_taxi" \ 
           -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data:rw 
           -p 5432:5432 \
         postgres:13
```
* Run the Notebook: ```upload_data.ipynb```. This is a modified version from the lecture to upload the new data.
* We can use this notebook to query the database using SQL.

## Question 3. Count records
How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

* 20689
* 20530
* 17630
* 21090

**Answer:** 20530

One possibilty to get the desired rows is the following SQL query:

```
query = """
SELECT \*, TO_CHAR(lpep_pickup_datetime,'YYYY-MM-DD') AS lpep_pickup_day, 
TO_CHAR(lpep_dropoff_datetime,'YYYY-MM-DD') AS lpep_dropoff_day
FROM green_taxi_data_2019
WHERE TO_CHAR(lpep_pickup_datetime,'YYYY-MM-DD')='2019-01-15' AND TO_CHAR(lpep_dropoff_datetime,'YYYY-MM-DD')='2019-01-15';
"""
pd.read_sql(query, con=engine)
```

## Question 4. Largest trip for each day
Which was the day with the largest trip distance Use the pick up time for your calculations.

* 2019-01-18
* 2019-01-28
* 2019-01-15
* 2019-01-10

**Answer:** 2019-01-15

One possibility to achieve this is the following SQL query:

```
query = """
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data_2019 t1
ORDER BY trip_distance DESC
"""
pd.read_sql(query, con=engine)
```

Then the first row is the largest trip distance. 

##Question 5. The number of passengers
In 2019-01-01 how many trips had 2 and 3 passengers?

* 2: 1282 ; 3: 266
* 2: 1532 ; 3: 126
* 2: 1282 ; 3: 254
* 2: 1282 ; 3: 274

**Answer:** 2: 1282 ; 3: 254

One possibility to achieve this is the following SQL query:

```
query = """
SELECT count(lpep_pickup_datetime)
FROM green_taxi_data_2019 
WHERE TO_CHAR(lpep_pickup_datetime,'YYYY-MM-DD')='2019-01-01'
GROUP BY passenger_count
"""
pd.read_sql(query, con=engine)
```

## Question 6. Largest tip
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's ```tip``` , not ```trip```

* Central Park
* Jamaica
* South Ozone Park
* Long Island City/Queens Plaza

**Answer:** Long Island City/Queens Plaza

One possible SQL query to achieve this is:

```
query = """
SELECT 
lpep_pickup_datetime,
lpep_dropoff_datetime,
tip_amount,
zpu."Zone" AS "pickup_zone",
zdo."Zone" AS "dropoff_zone"
FROM 
green_taxi_data_2019 t,
zones zpu,
zones zdo
WHERE
t."PULocationID" = zpu."LocationID" AND
t."DOLocationID" = zdo."LocationID" AND
zpu."Zone" = 'Astoria'
ORDER BY tip_amount DESC
"""
pd.read_sql(query, con=engine)
```
