# Batch Processing

## Introduction to Batch processing

* Batch vs. streaming
	* Batch: processing a chunk of data at regular intervals (e.g. daily, weekly,...)
	* Streaming: Processing data on the fly
* Types of batch jobs
	* SQL, Python scripts, **Spark**, Flink
* Orchestrating batch jobs
![workflow.png](workflow.png)
* Advantages and disadvabtages of batch jobs
	* Advantages: easy to manage, retry, scale; easier to orchestrate
	* Delay

## Spark Introduction

* What is Spark?
	* Spark is a "general purpose distributed **engine**"
	* open-source unified analytics engine for large-scale data processing
	* multi-language engine (e.g. wrapper fpr Python: PySpark)
	* Common use cases: batch-type workloads. (Also streaming, but we won't cover this here) 
* Why do we need it?
	* For the same things as you'd use SQL, but for executing the queries on the files in your datalake
* If you can write this in SQL and use Hive/Presto/Athena/BQ- do it. But not everything can/should be expressed in SQL
* Common case: ML algorithms. You can easily use SQL for most of it
* Typical Pipeline;
	* Raw data -> Data lake -> SQL -> Spark -> Spark for applying the model -> SQL
	![pipeline.png](pipeline.png)
	* All orchestrated with Airflow

## Installing Spark (Linux)

* Connecting to an instance on GCP and installing it there
* tested on Ubuntu 20.04
* Use the VM set up in week 1
* Start the VM and connect to it: ```ssh-i ~/.ssh/gcp froukje@external-ip```
* Install Java
	* To install Java we need a specific version of JDK (either 8 or 11)
	* Download from jdk.java.net/archive
	* Create a folder called "Spark" and download the file there```wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz```
	* Unpack it: ```tar xzvf openjdk-11.0.1_linux-x64_bin.tar.gz```
	* Create the variables: ```export JAVA_HOME="${HOME}/spark/jdk-11.0.1"``` and ```export PATH="${JAVA_HOME}/bin:${PATH}"```
* Install Spark
	* "spark.apache.org/downloads.html" select a release
	* ```wget https://dlcdn.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz```
	* ```tar xzfv spark-3.2.3-bin-hadoop3.2.tgz```
	* Do ```export SPARK_HOME="${HOME}/spark/spark-3.2.3-bin-hadoop3.2"``` and ```export PATH="${SPARK_HOME}/bin:${PATH}"```
	* run ```spark-shell``
	* To test, if everything is working run (in scala):
	```val data= 1 to 10000
	   val distData = sc.parallelize(data)
	   distData.filter(_ > 10).collect()```
	* To exit, run ```:quit```
* Put the export commands to the .bashrc file

## First look at Spark/PySpark

* Create a foleder called "notebooks"
* Connect to VM via: ssh -i ~/.ssh/gcp -L localhost:9999:localhost:9999 froukje@<external-ip>
* ```export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"```
* ```export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"```
* Note: Make sure that the version under ${SPARK_HOME}/python/lib/ matches the filename of py4j or you will encounter ModuleNotFoundError: No module named 'py4j' while executing import pyspark.
* In new terminal start from remote server: ```jupyter notebook -port=9999 --no-browser```
* Go to "localhost:9999" and enter the token
* Open a new Notebook, called "pyspark" and ``import pyspark``` and ```pyspark.__file__```. This gives: '/home/froukje/spark/spark-3.2.3-bin-hadoop3.2/python/pyspark/__init__.py'
* Download taxi data and use spark to read it: "wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
* Forward also port 4040: In new terminal: ssh -L localhost:4040:localhost:4040 froukje@<external-ip>
	* On localhost:4040, we can see all the spark jobs we executed

* Reading CSV files
* Partitions
* Saving data to Parquet for local experiments
* Spark master ui

## Spark DataFrames

* Actions vs transformations
* Partitions
* Functions and UDFs

## Spark SQL

* Temporary tables
* Some simple queries from week 4

## Joins in Spark

* Merge sort join
* Broadcasting

## RDDs

* From DF to RDD
* map
* reduce
* mapPartition
* From RDD to DF

## Spark Internals

* Driver, master and executors
* Partitioning & coalesce
* Shuffling
* Groupby or not groupby?
* Broadcasting

## Spark & Docker

## Running Spark in the Cloud (GCP)

* https://cloud.google.com/solutions/spark

## Connecting Spark to a DWH

* Spark with BigQuery (Athena/presto/hive/etc - similar)
* Reading from GCP and saving to BG

