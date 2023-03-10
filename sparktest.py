import os
import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql import SparkSession
from decouple import config

PYSPARK_HOME=config('PYSPARK_PYTHON')
HADOOP_HOME=config('HADOOP_HOME')
HADOOP_HOME_DIR=config('hadoop.home.dir')


try:
    class sparktester:
        def sparktester():
            spark = SparkSession.builder.getOrCreate()
            test_df = spark.createDataFrame([
                Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
                Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
                Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
            ])
            assert test_df

except Exception as e:
    print(f'There was an issue initializing Spark. Please rerun the script')
    print(e)
