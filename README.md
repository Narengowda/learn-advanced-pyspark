This pyspark app does these transformation

### Pipeline
- Uses the dataset : https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json
- Creates a table and make the output data ready to be queried after and during each execution using **Impala**, without any manual steps.
- It sends an alert if some Task failed, for instance: Send an email, a Slack message or anything else that makes error discovery and error handling easier.
- The executor rusn the whole pipeline or an individual Task by specifying it on the command line as an argument.

### Transformation

From the dataset It extracts all the of the recipes that have "*beef*" as one of the ingredients. Then, it adds a new field (named `difficulty`) calculates the difficulty of the recipe based on:

- **Hard** if the total of `prepTime` and `cookTime` is greater than 1 hour.
- **Medium** if the total is between 30 minutes and 1 hour.
- **Easy** if the total is less than 30 minutes.
- **Unknown** otherwise.

All of the data is stored in Parquet format

## Installation Steps
1. Install and setup spark, yarn, HDFS, hive and Impala.

        cd /home/nlakshma/spark/spark-2.4.0-bin-hadoop2.7

        SPARK_MASTER_IP=<your master IP/localhost> sudo sbin/start-master.sh
        sudo sbin/start-slave.sh spark://<your master IP/localhost>:7077
        
        # Make sure HDFS running if not run it
        /home/nlakshma/hadoop/sbin/start-dfs.sh
        
        # Make sure yarn running if not run it
        /home/nlakshma/hadoop/sbin/start-yarn.sh

        # verify the spark cluster by going to
        http://localhost:8080/
        
        # Check impala is running
        impala-shell
        #you should see
        [hostname:2100] > 
        
2. Install Python 2.7
3. Install virtualenv
4. Create a virtualenv: virtualenv ~/virtual_envs/hello_fresh
5. pip install -r requirements.txt
6. Download recipes.json from amazons3 and upload it to HDFS by

        hdfs dfs -put recipes.json /
7. go to code repo

        cd recipes-etl


## How to run?
#### Spark in local mode
    python app.py

#### SPARK in standalone mode:

    PYSPARK_PYTHON=/home/nlakshma/helloenv/bin/python2 spark-submit --jars "driver/ImpalaJDBC4.jar" app.py --master="spark://blr-lpa6q.bangalore.corp.akamai.com:7077"
    
#### SPARK in Yarn client mode:    
    PYSPARK_PYTHON=/home/nlakshma/helloenv/bin/python2 spark-submit --jars "driver/ImpalaJDBC4.jar" app.py --master="yarn-client"

#### Run tests
	python -m unittest discover tests

### How you would schedule the system in order to run automatically at a certain time?
I will use Oozie workflow and coordinator to schedule spark job.
http://archive.cloudera.com/cdh4/cdh/4/oozie/CoordinatorFunctionalSpec.html#a1._Coordinator_Overview

### Module explanation
#### app.py
Entry point of the app, which parses the arguments of the commandline application
    python app.py will show usage info

	(helloenv) naren@ubuntu:~/test/test/test/Narengowda-data-engineering/recipes-etl$ python app.py --help
	[nltk_data] Downloading package punkt to /home/nlakshma/nltk_data...
	[nltk_data]   Package punkt is already up-to-date!
	usage: app.py [-h] [--tasks EXEC_TASKS] [--email EMAIL] [--master MASTER]

	Pyspark executor

	optional arguments:
	  -h, --help          show this help message and exit
	  --tasks EXEC_TASKS  list of tasks to execute, select from list
						  RecipesTransformationTask,SaveRecipesParquetTask
	  --email EMAIL       Email Id to send notification
	  --master MASTER     Spark Master

pass tasks in comma separated value of, if no tasks are specified all tasks in registered pipeline will will run

#### data_models.py
This module holds the Recipe models with all of the logic specific to the recipe entry.
like condition to check for is recipe or not, assiginig the difficulty score or 
its sql schema and method to convert to sql row.

#### Tasks.py
Contains the abstraction of tasks and different tasks according to the requirements

## spark_utils.py
This is where spark connection creation logic is written 


