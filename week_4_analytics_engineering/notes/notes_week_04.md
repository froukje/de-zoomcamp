# Analytics Engineering

## What is Analytics Engineering?

![analytics_engineer.png](analytics_engineer.png)

![tools.png](tools.png)

* We will focus on
	* Data modelling
	* Data presentation

* Data modelling concepts
	* ETL vs ELT
		* In ETL the data is transformed before loading to the data warehouse
		* In ETL the data is tranformed in the data warehouse
		* ETL is more stable
		* ELT is faster and more flexible

	![etl_elt.png](etl_elt.png)

	* Kimball's Dimensional Modeling:
		* Objective:
			* Deliver data, that is understandable to the business users
			* Deliver fast query performance

		* Approach:
			* Prioritise user understandability and query performance over non redundand data (3NF)
		* Other approaches:
			* Bill Inmon
			* Data vault
	* Elements of Dimensional Modeling
		* Facts tables
			* Measurements, metrics or facts
			* Corresponds to a business process
			* "verbs" (e.g. sales, orders)
		* Dimensions tables
			* Corresponds to a business entity
			* Provides context to a business process
			* "nouns" (e.g. customer, product)
		* Also known as star schema
	
		![star_schema.png](star_schema.png)

	* Architecture of Dimensional Modeling
		* Stage Area
			* Contains the raw data
			* Not meant to be exposed to everyone
		
		* Processing area
			* From raw data to data models
			* Focuses in efficency
			* Ensuring standards
		
		* Presentation area
			* Final presentation of the data
			* Exposure to business stakeholder

		* Restaurant analogy:
			* Stage area: food is processed (limited to people who know how to do it)
			* Processing area: kitchen (limited to people who know how to do it)
			* Presentation area: Dining area (exposure)

## What is dbt?

* dbt is a transformation tool that allows anyone that knows SQL to deploy analytics code following software engineering best practices like modularity, partability, CI / CD, and documentation.
![dbt.png](dbt.png)

* dbt workflow
	* develop models
	* test and document them
	* deployment phase (version control and CI / CD)

![dbt_workflow.png](dbt_workflow.png)

* How does dbt work?	
	* Add modeling layer to our data, where the data is transformed
	* The transformed data is persist back to the data warehouse
	* The transformation is doen with SQL, which are our dbt models
	* dbt compiles the code and pushes to the data warehouse
	![dbt2.png](dbt2.png)

## How to use dbt?

* dbt Core
	* Open source project, that allows data transformations
	* Builds and runs a dbt project (.sql, and .yml files)
	* Includes a CLI interface to run dbt commands locally
	* Opens source and free to use
* dbt Cloud
	* SaaS application to develop and manage dbt projects
	* Web-based IDE to develop, run and test a dbt project
	* Jobs orchestration
	* Logging and alerting
	* Integrated documentation 
	* Free for individuals (one developer seat)

## How are we going to use dbt?

* BigQuery
	* Development using cloud IDE
	* No local installatio of dbt core
* Postgres
	* Development using a local IDE of your choice
	* Local installation of dbt core connecting to the Postgres database
	* Running dbt models through the CLI
	![dbt3.png](dbt3.png)

## Starting a dbt project
* dbt provides a starter project with all the basic folders and files.
* There are essentially two ways to use it:
	* With the CLI
		* After having installed dbt loacally and setup the profiles.yml, run ```dbt_init``` in the path we want to start the project to clone the starter project
	* With dbt cloud
		* After having set up dbt cloud credentials (repo and dwh) we can start the project from the web-based IDE
			
* Upload needed data (if not already done)
	* In case data is missing here is a script for uploading https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/extras/web_to_gcs.py
* In Google BigQuery create two new tables (schemas) in our project: "dbt-some-name" (for development), "production" (for production)
* Make a dbt-cloud account and connect to BigQuery: https://www.getdbt.com/signup/
* Create a service account for dbt in BigQuery following this instruction: https://github.com/froukje/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md
	* Note to add the dbt key to the github repo, go to your repo and click on settings in the top menu, not below your profile
* Create a new branch on github 
* After finishing the setup go to "Develop" in the dbt cloud
* Switch to the new branch 
* Initialize the project by clicking the button
* This creates several folders and files
* Open the dbt_project.yml
	* Change the name to e.g. "taxi_rides_ny"
	* Change "models" to "taxi_rides_ny"
	* Delete the last to rows "example ...", we are not using this here


