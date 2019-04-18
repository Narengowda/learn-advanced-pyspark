from executor import Executor
from tasks import BaseTask
from spark_utils import setup_spark
import unittest
import mock


class FakeTask(BaseTask):
    def execute(self):
        pass


class FakeFailTask(BaseTask):
    def execute(self):
        raise Exception()


class TestExecutorTest(unittest.TestCase):
    @mock.patch("executor.setup_spark", return_value="")
    def test_executor(self, mock_func):
        executor = Executor("a@b.com", "master", tasks=[FakeTask])
        self.assertEquals(executor.run(), True)
        mock_func.assert_called_with(
            app_name="hello_fresh_etl",
            master="master",
            py_files=[
                "data_models.py",
                "datastore.py",
                "executor.py",
                "configuration.py",
                "tasks.py",
            ],
            spark_jars=["driver/ImpalaJDBC4.jar"],
        )

    @mock.patch("executor.setup_spark", return_value="")
    def test_executor_fail(self, mock_func):
        executor = Executor("a@b.com", "master", tasks=[FakeFailTask])
        self.assertEquals(executor.run(), False)

    @mock.patch("executor.setup_spark", return_value=9)
    def test_executora_multi_fail(self, mock_func):
        executor = Executor("a@b.com", "master", tasks=[FakeFailTask, FakeTask])
        self.assertEquals(executor.run(), False)
