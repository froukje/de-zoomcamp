# de-zoomcamp

This repository contains notes and exercises I made taking the Data Engineer Zoomcamp provided by the [Data Talks Club](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Content

Data used: [Yellow Taxi Data New York](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

The data can ce downloaded using: ```wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv```

### Week 1: Introduction & Prerequisites 
* Postgres
	* Load the data into a database
	* Use pgcli to connect to Postgres
* pgAdmin
	* Use the webinterface to look at the data
* Docker
	* Getting started with Docker
	* Use Docker to start Postgres
	* Use Docker to start pgAdmin
	* Use both in the same network
* docker-compose
	* Use one yaml-file to start pgAdmin and Postgres in the same network
* Introduction to Terraform
* Introduction to Google Cloud
* Homework

### Week 2: Workflow Orchestration

* Data Lake
* Workflow orchestration
* Introduction to Prefect
* ETL with GCP & Prefect
	* store data in GCS and Big Query
* Parametrizing workflows
* Prefect Cloud and additional resources
* Homework

### Week 3: Data Warehouse

* Data Warehouse
* BigQuery
* Partitioning and clustering
* BigQuery best practices
* Internals of BigQuery
* Integrating BigQuery with Airflow
* BigQuery Machine Learning

### Week 4: Analytics Engineering

* Basics of analytics engineering
* dbt (data build tool)
* BigQuery and dbt
* Postgres and dbt
* dbt models
* Testing and documenting
* Deployment to the cloud and locally
* Visualizing the data with google data studio and metabase

### Week 5: Batch processing

* Batch processing
* What is Spark
* Spark Dataframes
* Spark SQL
* Internals: GroupBy and joins

### Week 6: Streaming

* Introduction to Kafka
* Schemas (avro)
* Kafka Streams
* Kafka Connect and KSQL

### Week 7, 8 & 9: Project
