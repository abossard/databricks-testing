
import random
import time
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

def do_work(item):
  print("Work for Job: " + str(item))
  delay = random.randint(1,20)
  time.sleep(delay)
  result = dict(**item)
  result.update(delay=delay)
  return result

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    #sc = SparkContext(master='local[2]', appName=__name__)
    
    jobinput = [dict(name="job_" + str(y))  for y in range(10)]
    ji_df = sc.parallelize(jobinput, 10)
    result = ji_df.map(do_work).collect()
    print(result)
