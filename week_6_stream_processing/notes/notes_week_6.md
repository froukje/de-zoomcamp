# Stream Processing

**What is stream processing? What is Kafka? How does Kafka play a role in strem processing?**

## What is Data Exchange?

* Traditional data exchange: e.g. Postal Service
* In the context of computers: APIs
	* The idea remains the same.
	* One computer shares information (data), which is exchanged by a service
* Data exchange in stream processing
	* Data is exchanged in more real-time 
	* Not immideately, but a few seconds of delay
	* Compared to batch-processing, which is houry, daily or ...

## What Kafka Streaming?
* Data exchange can be managed by topics, e.g. consumers only receive the intormation (data) for a specific topic they are interested in (e.g. newsletter)
![topics.png](topics.png)
* Kafka uses the name "topic" a lot
* What is a topic (in Kafka)?
	* continous stream of events
* What is an event?
	* events over time: single data points at a certain timestamp
	* each event contains a message
	* a message can have different properties, required fields, not required fields
	* a message has three structures
		* key: (used for partitioning)
		* value: value for the data exchange
		* timestamp
* The collection of events go into our topic, and these evens are then read by other consumers
* logs in Kafka
	* The way events are stored
* What does Kafka provide, that other streaming platforms / APIs don't provide?
	* robustness & reliability (even if servers go down, you will still receive the data)
	* flexibility (topics can be small or big, one, few or a lot of consumers are possible)
* Topics can be read by other consumers, even it was already read by a consumer 

## Confluent Cloud free trial 

* (free for 30 days)
* Allows to have a Kafka cluster
* https://www.confluent.io/confluent-cloud/
* Create a cluster
	* Choose "free"
	* Choose "Google Cloud" and a region (Frankfurt(europe-west3), single zone
	* Skip payment options
	* Create Cluster
* API Keys
	* Everytime we do something with confluent cloud, we need an API Key
	* Create a global key and give it a name
* Create a firs topic for testing
	* Go to link "Topics" -> "Create topic"
	* Choose a name and nr of Partitions, for this tutorial choose 2 partitions
	* In "Advanced Settings" 
		* Choose "Retention time": 1 day
	* Create the topic and go to "Messages"
		* keep the default Message and click "Produce"
	* Create a connector
		* Use "Datagen Source"
		* Choose output record value format: JSON
		* Select a template: Orders
		* Choose a "Connector name"
	* We can then see that data is processed

## Kafka producer consumer

* Produce messages programmatically to Kafka
* Use taxi data
* Use Java
* Create a Topic, call it "rides" and choose "Partitions": 2
	* Advanced Settings: Choose Retention time: 1 day
		* This means all messages older than 1 day will be deleted
	* save & create
* Connect the topic to a client
	* Click on "Clients"
	* Choose Java
* "Ride.java" reads the string data and converts them to an object in Java
* "JsonProducer.java":
	* reads rides.csv file
	* produces messages, which we can see in the confluent cloud
* "JsonConsumer.java" is a consumer

## Kafka configuration

* Kafka Cluster
	* nodes / machines talking to each other 
* Topic
	* sequence of events
	* consists of a message, which contains a key, a value and a timestamp
* Partitions
	* Data is stored in different partitions, which can communicate with the consumer independingly 
## Kafka streams basics

## Kafka stream join

## Kafka stream testing

## Kafka stream windowing

## Kafka ksqldb & Connect

## Kafka Schema registry


