"""Place to write tasks"""

import json
import smtplib
import logging
import time

from configuration import IMPALA
from data_models import Recipe
from datastore import ImpalaDatastore
from impala.error import HiveServer2Error


class Logger(object):
    """For now simple logger"""

    def log(self, message):
        # Print !!, to keep it simple
        print(">>>>>>>>INFO: {}".format(message))

    def error(self, err):
        # Print !!, to keep it simple
        print(">>>>>>>>>>>>>>>> ERROR: {}".format(err))


class Notify(Logger):
    """Class to handle notifications, for now only email is added"""

    def __init__(self, owner):
        self.sender = "hello@fresh.com"
        self.owner = owner

    def notify(self, to, message):
        receivers = [to]
        try:
            smtp_obj = smtplib.SMTP("localhost")
            smtp_obj.sendmail(self.sender, receivers, message)
        except smtplib.SMTPException:
            pass
            # Commenting as of now as I dont know valid smtp server
            # self.error("Error: unable to send email")

    def success(self):
        """Handle success task execution notification"""
        self.notify(self.owner, "Task success")

    def failure(self, err):
        """Handle task error notification"""
        self.notify(self.owner, "Task failure {}".format(err))


class BaseTask(Notify):
    """Base task with common methods"""

    DRIVER = "com.cloudera.impala.jdbc4.Driver"

    def __init__(self, spark_session, owner, config):
        super(BaseTask, self).__init__(owner)
        self.spark_session = spark_session
        self.impala_connection_str = config.IMPALA

    def __call__(self):
        self.log(" ### Starting task {} ###".format(self.__class__.__name__))
        status = False
        try:
            self.execute()
            self.success()
            status = True
        except Exception as ex:
            self.error(ex)
            self.failure(str(ex))
        self.log("___ finishing task {} ___".format(self.__class__.__name__))
        return status

    def execute(self):
        """Entry point to the task"""
        raise NotImplementedError("Implement execute()")

    def load_file(self, file_path):
        """Loads the text file from given path"""
        return self.spark_session.sparkContext.textFile(file_path)

    def create_table(self, table, columns):
        """
        Looks like there is some issue with Impala JDBC driver
        CREATE TABLE test_two ("x" INTEGER )
        String literal is emited instead of x
        Due to which, STRING LITERAL error is fired
        taking alternative route, creating table manually.
        """
        columns = [k + " " + v for k, v in columns.items()]
        columns = ",".join(columns)
        CREATE = "CREATE TABLE {table} ({columns});".format(
            table=table, columns=columns
        )

        impala = ImpalaDatastore()
        impala.connect()

        try:
            impala.execute(CREATE, fetch=False)
        except HiveServer2Error as ex:
            # No Table exists exception is available
            # so handeling the dirty way.
            if not "Table already exists" in str(ex):
                raise HiveServer2Error(ex)
            else:
                self.log("looks like table already exists")

    def save_parquet(self, df, file_path):
        return df.write.parquet(file_path)

    def convert_data(self, recipes):
        rows = map(lambda recipe: recipe.to_row(), recipes.collect())
        return rows

    def write_table(self, df, table, mode="append"):
        out = (
            df.write.format("jdbc")
            .mode(mode)
            .options(dbtable=table, **self.db_options())
        )
        out.save()

    def load_df(self, table):
        return (
            self.spark_session.read.format("jdbc")
            .options(dbtable=table, **self.db_options())
            .load()
        )

    def db_options(self):
        return {"url": self.impala_connection_str, "driver": self.DRIVER}


class RecipesTransformationTask(BaseTask):
    """Task which processes recipes and saves it to database"""

    TABLE_NAME = "recipe_data"
    TABLE_SCHEMA = {
        "summary": "VARCHAR",
        "cooking_time": "integer",
        "image": "VARCHAR",
        "ingredients": "VARCHAR",
        "name": "VARCHAR",
        "preparation_time": "integer",
        "url": "VARCHAR",
        "date_of_execution": "VARCHAR",
        "difficulty": "VARCHAR",
    }

    def format_data(self, recipes):
        """Does data conversion and filters the data"""
        recipes = recipes.map(lambda recipe: json.loads(recipe))
        recipes = recipes.map(lambda recipe_json: Recipe(recipe_json))
        recipes = recipes.filter(lambda recipe: recipe.is_receipe("beef"))
        return recipes

    def execute(self):
        """Executes the RecipesTransformationTask"""
        data = self.load_file("hdfs://localhost:9000/recipes.json")
        recipes = self.format_data(data)
        recipes_rows = self.convert_data(recipes)
        recipe_df = self.spark_session.createDataFrame(recipes_rows)
        self.create_table(self.TABLE_NAME, self.TABLE_SCHEMA)
        self.write_table(recipe_df, self.TABLE_NAME)
        self.log("saved the data to Impala ")


class SaveRecipesParquetTask(BaseTask):
    """Task to save the data in Impala table to parquet file"""

    OUTPUT_FILE_NAME = "hdfs://localhost:9000/output/{}/recipes_proto.parquet"
    TABLE_NAME = "recipe_data"

    def execute(self):
        """SaveRecipesParquetTask Task entry point"""
        df = self.load_df(self.TABLE_NAME)
        output_file_name = self.OUTPUT_FILE_NAME.format(int(time.time()))
        df.write.partitionBy("difficulty").parquet(output_file_name)
        self.log("saved the parquet file at {}".format(output_file_name))
