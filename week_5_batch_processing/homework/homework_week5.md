## Week 5 Homework

In this homework we'll put what we learned about Spark in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

### Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session
* Execute spark.version
* What's the output?

* 3.3.2
* 2.1.4
* 1.2.3
* 5.4

**Answer:** '3.2.3'

### Question 2. HVFHW February 2021

HVFHW June 2021

Read it with Spark using the same schema as we did in the lessons.
We will use this dataset for all the remaining questions.
Repartition it to 12 partitions and save it to parquet.
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

* 2MB
* 24MB
* 100MB
* 250MB

**Answer:** 24MB

### Question 3. Count records

**Count records**

How many taxi trips were there on June 15?

Consider only trips that started on June 15.

* 308,164
* 12,856
* 452,470
* 50,982

**Answer:** 452,470

### Question 4. Longest trip for each day

**Longest trip for each day**

Now calculate the duration for each trip.
How long was the longest trip in Hours?

* 66.87 Hours
* 243.44 Hours
* 7.68 Hours
* 3.32 Hours

**Answer:** 66.87 hours 

### Question 5. Most frequent dispatching_base_num

**User Interface**

Spark’s User Interface which shows application's dashboard runs on which local port?

* 80
* 443
* 4040
* 8080

**Answer:** 4040

### Question 6. Most common locations pair

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?

* East Chelsea
* Astoria
* Union Sq
* Crown Heights North

**Answer:** Astoria
