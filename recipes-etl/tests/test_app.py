from executor import Executor, BaseExecutor
from tasks import BaseTask
from spark_utils import setup_spark
import unittest
import mock
import app

# Not writing tests for all of the senarios due to lack of time(sorry about that)
# but these tests should give an idea about, how to test.


class TestApp(unittest.TestCase):
    def setUp(self):
        self.orig = mock.MagicMock()

    @mock.patch("app.Executor")
    def test_run(self, mocked_executor):
        self.args = mock.MagicMock()
        self.args.email = "a@v.com"
        self.args.master = "master"
        self.args.exec_tasks = "RecipesTransformationTask"
        app.run(self.args)

        # Check only RecipesTransformationTask is called
        self.assertIn("RecipesTransformationTask", str(mocked_executor.call_args))
        self.assertNotIn("SaveRecipesParquetTask", str(mocked_executor.call_args))

    @mock.patch("app.Executor")
    def test_default_pipeline(self, mocked_executor):
        self.args = mock.MagicMock()
        self.args.email = "a@v.com"
        self.args.master = "master"
        self.args.exec_tasks = ""

        app.run(self.args)
        mocked_executor.assert_called()
        self.assertIn("RecipesTransformationTask", str(mocked_executor.call_args))
        self.assertIn("SaveRecipesParquetTask", str(mocked_executor.call_args))
