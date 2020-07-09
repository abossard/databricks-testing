
import random
import time
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

def do_work(item):
  print("Work for Job: " + str(item))
  delay = random.randint(1,2)
  time.sleep(delay)
  result = dict(**item)
  result.update(delay=delay)
  return result

def execute_spark_load(sc, input):
  jobinput = input
  ji_df = sc.parallelize(jobinput, 10)
  return ji_df.map(do_work).collect()

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    print(execute_spark_load(sc, [dict(name="job_" + str(y))  for y in range(10)]))
    
    