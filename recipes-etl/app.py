"""Spark app to run spark jobs"""

import argparse

from collections import OrderedDict
from executor import Executor
from tasks import RecipesTransformationTask, SaveRecipesParquetTask

# Tasks Pipeline
# Register your tasks here.
TASK_REGISTER = [
    ("RecipesTransformationTask", RecipesTransformationTask),
    ("SaveRecipesParquetTask", SaveRecipesParquetTask),
]

def run(args):
    """Entry point function, which takes args and executes the tasks"""
    email = args.email
    master = args.master

    # If user hasn't passed any arguments, run the default tasks
    if not args.exec_tasks:
        tasks = [x[1] for x in TASK_REGISTER]
        tasks_names = "".join([x[0] for x in TASK_REGISTER])
        print(
            "No specific tasks are given, so running default pipeline {}".format(
                tasks_names
            )
        )
    else:
        try:
            exec_tasks = [x.strip() for x in args.exec_tasks.split(",")]
            tasks = [dict(TASK_REGISTER)[task] for task in exec_tasks]
        except KeyError:
            print("Wrong task given")
            return

    try:
        # Call the executor
        ex = Executor(owner=email, tasks=tasks, master=master)
        ex.run()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyspark executor")
    default_tasks = ",".join([x[0] for x in TASK_REGISTER])

    # Add arguments
    parser.add_argument(
        "--tasks",
        dest="exec_tasks",
        default=default_tasks,
        help="list of tasks to execute, select from list {}".format(default_tasks),
    )
    parser.add_argument(
        "--email",
        dest="email",
        default="admin@hello.com",
        help="Email Id to send notification",
    )
    parser.add_argument(
        "--master", dest="master", default="local[*]", help="Spark Master"
    )

    args = parser.parse_args()
    run(args)
