"""Utility functions"""

import os
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark import SparkConf


def setup_spark(
    app_name="my_app", master="local[*]", spark_jars=[], spark_config={}, py_files=[]
):
    """sets up spark app using configuration provided"""

    cwd = os.getcwd()
    conf = SparkConf().setAppName(app_name).setMaster(master)

    conf = conf.set("spark.jars", ",".join(spark_jars))

    # update spark config
    for key, val in spark_config.items():
        conf.set(key, val)

    for i in conf.getAll():
        print(i[0], "-->", i[1])

    SparkSession.builder.config(conf=conf)
    spark_session = SparkSession.builder.appName(app_name).getOrCreate()

    for pyf in py_files:
        spark_session.sparkContext.addPyFile(pyf)

    return spark_session
