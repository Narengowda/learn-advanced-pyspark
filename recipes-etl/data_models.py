"""Data models for Recipe"""
import json
import nltk

from nltk.tokenize import word_tokenize
from pytimeparse.timeparse import timeparse

nltk.download("punkt")
from datetime import datetime
from dateutil import parser
from pyspark.sql.types import *
from pyspark.sql import Row


class LEVELS(object):
    """Different levels of difficulty"""
    UNKNOWN = "UNKNOWN"
    HARD = "HARD"
    MEDIUM = "MEDIUM"
    EASY = "EASY"


class Recipe(object):
    """Data model for recipe"""

    def __init__(self, args):
        self.name = args.get("name").encode("ascii", errors="ignore") or "-"
        self.url = args.get("url").encode("ascii", errors="ignore") or "-"
        self.image = args.get("image").encode("ascii", errors="ignore") or "-"
        self.recipe_yield = (
            args.get("recipeYield").encode("ascii", errors="ignore") or "-"
        )
        self.ingredients = (
            args.get("ingredients").replace("\n", "").encode("ascii", errors="ignore")
            or "-"
        )
        self.description = (
            args.get("description").replace("\n", "").encode("ascii", errors="ignore")
            or "-"
        )
        self.cooking_time = -1
        self.preparation_time = -1

        if args.get("prepTime"):
            self.preparation_time = timeparse(args["prepTime"].replace("PT", ""))

        if args.get("cookTime"):
            self.cooking_time = timeparse(args["cookTime"].replace("PT", ""))

    def is_receipe(self, target="beef"):
        """
        Method to check is a type of receipe

        Args:
            target: text to match
        returns:
            Boolean flag of target string present or not
        """
        ingredients = word_tokenize(self.ingredients)
        ingredients = [w.lower() for w in ingredients]
        return target.lower() in ingredients

    @property
    def difficulty(self):
        """
        Method to access the difficulty of the recipe

        returns:
            difficulty level
        """
        if -1 in [self.preparation_time, self.cooking_time]:
            self.preparation_time = (
                self.preparation_time if self.preparation_time else 0
            )
            self.cooking_time = self.cooking_time if self.cooking_time else 0

            duration = self.preparation_time + self.cooking_time

            if duration > 3600:
                return LEVELS.HARD
            elif duration > 1800:
                return LEVELS.MEDIUM
            elif duration > 0:
                return LEVELS.EASY

        return LEVELS.UNKNOWN

    def to_row(self):
        """Converts the object to row

        :return: Spark SQL Row
        """
        # Known issue in Impala:
        #   https://community.cloudera.com/t5/Interactive-Short-cycle-SQL/HIVE-PARAMETER-QUERY-DATA-TYPE-ERR-NON-SUPPORT-DATA-TYPE/m-p/79109/highlight/true?device-view=desktop
        return Row(
            ingredients=self.ingredients[:500],
            name=self.name,
            url=self.url,
            difficulty=self.difficulty,
            image=self.image,
            preparation_time=self.preparation_time,
            cooking_time=self.cooking_time,
            date_of_execution=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        )

    def get_schema(self):
        """
        Builds the schema for Recipe

        :return:
            Schema of recipe
        """
        return StructType(
            [
                StructField("ingredients", StringType(), True),
                StructField("difficulty", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("image", StringType(), True),
                StructField("preparation_time", IntegerType(), True),
                StructField("cooking_time", IntegerType(), True),
                StructField("date_of_execution", StringType(), True),
            ]
        )

    def __str__(self):
        """Minimal String representation of the data"""
        return json.dumps(
            {
                "name": self.name,
                "url": self.url,
                "image": self.image,
                "ingredients": self.ingredients,
                "is_beef_receipe": self.is_receipe("beef"),
            }
        )
