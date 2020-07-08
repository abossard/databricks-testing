
import random
import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

print("Result: ")
print(spark.range(1000000000).count())