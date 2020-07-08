
import random
import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()

def do_work(item):
  print("Work for Job: " + str(item))
  delay = random.randint(1,10)
  time.sleep(delay)
  result = dict(**item)
  result.update(delay=delay)
  return result

jobinput = [dict(name="job_" + str(y))  for y in range(100)]
ji_df = spark.sparkContext.parallelize(jobinput, 100)
result = ji_df.map(do_work).collect()
print(result)