from executor import Executor
from tasks import RecipesTransformationTask, BaseTask
from spark_utils import setup_spark
import unittest
import mock


class TestTask(unittest.TestCase):
    def setUp(self):
        self.config = mock.MagicMock()
        self.config.IMPALA = "xxx"
        self.create_table = mock.MagicMock()
        self.format_data = mock.MagicMock(return_value=[{"test": "test"}])
        self.load_file = mock.MagicMock(
            return_value=[
                {
                    "name": "Pasta",
                    "ingredients": "Diced",
                    "url": "h",
                    "image": "u",
                    "cookTime": "PT10M",
                    "recipeYield": "8",
                    "datePublished": "2011-06-06",
                    "prepTime": "PT6M",
                    "description": "desc",
                }
            ]
        )

    def test_recipes_transform(self):
        task = RecipesTransformationTask(object, "owner", self.config)
        task.load_file = self.load_file
        task.format_data = self.format_data
        task()
        self.format_data.assert_called_with(
            [
                {
                    "description": "desc",
                    "recipeYield": "8",
                    "name": "Pasta",
                    "ingredients": "Diced",
                    "url": "h",
                    "image": "u",
                    "datePublished": "2011-06-06",
                    "prepTime": "PT6M",
                    "cookTime": "PT10M",
                }
            ]
        )

    def test_execute_abstract(self):
        task = BaseTask(object, "owner", self.config)
        with self.assertRaises(NotImplementedError):
            task.execute()

    def test_db_options(self):
        task = BaseTask(object, "owner", self.config)
        self.assertEqual(
            task.db_options(),
            {"driver": "com.cloudera.impala.jdbc4.Driver", "url": "xxx"},
        )
