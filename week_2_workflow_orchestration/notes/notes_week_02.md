# Data Lake

## What is a Data Lake?

* holds big data from many sources
* data can be structured, semi-structured or unstructured
* idea: ingest data as quickly as possible and make it accessable to other team members
* has to be secure and scalable, inexpensive hardware

## Data Lake vs. Data Warehouse

* Data Lake:
	* generally unstructered data
	* target users: Data Scintists, Data Analysts, ...
	* stores huge amount of data
	* use cases: stream processing, ML, real-time analytics
* Data Warehouse:
	* data is generally structured
	* target users: Business Analysts
	* small data size
	* use cases: batch processing, (...)

## How did Data Lakes started?
	* companies realized the value of data
	* store and access quickly
	* cannot always define structured data
	* usefulness of data being realized later in the project lifecycle
	* increase in Data Scientists
	* R&D on data products
	* need for cheap storage of big data

## ETL vs. ELT
	* ETL: Export Transform and Load (Data Warehouse solution)
	* ELT: Export Load and Transform (Data Lake solution)
	* ETL is mainly used for a small amount of data, whereas ELT is used for large amounts of data
	* ELT provides data lake support

## Gotcha of Data Lake
	* converting into Data Swamp (-> makes it hard to be useful for e.g. Data Scientists)
	* no versioning
	* incompatible schemas for same data without versioning
	* no metadata associated
	* joins not possible

## Cloud provider Data Lake
	* GPC - cloud storage
	* AWS - S3
	* AZURE - AZURE BLOB

# Introduction to Workflow Orchestration
	* workflow orchestration means governing the dataflow in a way that respects orchestration rules and business logic
	* dataflow binds otherwise dispared set of applications together
	* workflow orchestration allows to turn any code into a scheduled workflow, that can be run and observed
	* core features of workflow orchestration:
		* remote execution
	 	* scheduling	
		* retries
		* caching
		* integration with external systems (APIs, databases)
		* ad-hoc runs
		* parametrization
		* alert, when something fails

# Introduction to Prefect
## Use case: python script that pulls yellow taxi data into a postgres db 
	* 'de-zoomcamp/week_2_workflow_orchestration/videos/ingest_data.py'
	* create a conda environment: ```conda create -n zoom python=3.9``` and activate it ```conda activate zoom```
	* install dependencies: ```pip install -r requirements.txt```
	* run docker-compose.yaml file from week1 (week_1_basics_n_setup/videos/2_docker_sql/docker_postgres): ```docker-compose up```	and then ```python ingest_data.py```
	* now we can go to ```localhost:8080``` and log into pgadmin
	* create a new server: name "Docker localhost", port: "5432", host: "pgdatabase"
## Next: run the script on a schedule
	* transform the script into a prefect flow: ```ingest_data_flow.py```
	* flow:
		* most basic prefect object
		* container for workflow logic
		* allows to interact with the other workflow
	* make needed imports:```from prefect import flow, task```
	* define a new function called ```main_flow```, here the variables are set and the ```ingest_data``` function is called
	* turn this into a prefect flow, by decorating it with ```@flow```
	* flows can take tasks, we then transform the ```ingest_data``` function into a task by decorating it with ```@task```
	* transform the script to ETL	
		* that is reorganize it in 3 functions for extracting, transforming and loading
	* we can paramterize the flow, i.e. give the parameters to the flow instead of defining them inside, e.g. "table_name"
	* a flow can have subflows (e.g. "log_subflow")
	* we can use prefect ui to visualize the flows
		* open a new terminal and type ```prefect orion start```
		* to be sure to point to the correct API: ```prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api```
		* browse to localhost:4200 to the the flows and tasks in the ui
	* Blocks:
		* enable storage of configuration and provides an interface for interacting with the external system
		* several different types of blocks you can build, e.g. run infrastructure, credentials, read / write paths, GCS
		* can create your own blocks aswell				
		* different blocks can build upon each other, e.g. a credential block can be used in a bucket block
	* Transform our credentials (user, password, etc.) into a SQL Alchemy block
		* prefect SQLAlquemy is already installed
		* see documentation: prefecthq.github.io/prefect-sqlalchemy
		* to register the block: ```prefect block register -m prefect_sqlalchemy``` (necessary if not shown in the ui)
		* the block we use is called ```SQLAlchemy Connector``` in the ui
		* click on "add"
		* give the block a name: "postgres-connector"
		* use "SyncDriver", choose "postgresql+psycopg2"
		* set the name of the db (according to what we chose in the python script) "ny_taxi", username: "root", password: "root", host: "localhost", port: 5432	
		* press "create"
		* Now, import this to our script
		```from prefect_sqlalchemy import SqlAlchemyConnector```
		* adapt the script accordingly to load the data
		* blocks can also be added using code (see documentation)
