"""Executor logic"""

import os
import configuration

from abc import ABCMeta, abstractmethod
from spark_utils import setup_spark
from tasks import BaseTask


class BaseExecutor(object):
    __metaclass__ = ABCMeta

    def __init__(self, master, tasks=[]):
        """Initialize the base executor"""
        self.tasks = tasks
        self.master = master
        self.spark_session = self.setup_sparksession()
        self.dataframe = None

    def setup_sparksession(self):
        """
        Setup spark session

        :return
            returns spark session
        """

        py_files = [
            "data_models.py",
            "datastore.py",
            "executor.py",
            "configuration.py",
            "tasks.py",
        ]

        return setup_spark(
            app_name="hello_fresh_etl",
            master=self.master,
            spark_jars=["driver/ImpalaJDBC4.jar"],
            py_files=py_files,
        )

    @abstractmethod
    def run(self):
        """Inherited classes must implement run"""


class Executor(BaseExecutor):
    def __init__(self, owner, master, tasks=[], *args, **kwargs):
        """Initialize the executor"""
        self.owner = owner
        self.tasks = tasks
        self.master = master
        super(Executor, self).__init__(master, tasks, *args, **kwargs)

    def run(self):
        """
        Executes the list of tasks

        :return:
            Returns the status of the all of the tasks
        """
        for t in self.tasks:
            task = t(self.spark_session, self.owner, configuration)

            if not isinstance(task, BaseTask):
                raise ValueError("Wrong task data type")

            status = task()
            if not status:
                # looks like tasks has failed
                return status

        return status
