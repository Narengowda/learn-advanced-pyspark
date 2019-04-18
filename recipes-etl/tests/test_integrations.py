import unittest
import logging
import configuration
import mock
from pyspark.sql import SparkSession
from tasks import RecipesTransformationTask


# Not writing tests for all of the senarios due to lack of time(sorry about that)
# but these tests should give an idea about, how to test.

class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
	logger = logging.getLogger('py4j')
	logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
	return (SparkSession.builder
	.master('local[2]')
	.appName('my-local-testing-pyspark-context')
	.enableHiveSupport()
	.getOrCreate())

    @classmethod
    def setUpClass(cls):
	cls.suppress_py4j_logging()
	cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
	cls.spark.stop()


class TestRecipeApp(PySparkTest):

    @mock.patch('tasks.ImpalaDatastore')
    def test_tasks(self, mockds):
	task = RecipesTransformationTask(self.spark, 'naren@hello.com', configuration)
	task.write_table = mock.MagicMock()
        return_value = task()
        self.assertTrue(return_value)



