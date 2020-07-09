#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()
#sc = spark.sparkContext

import os
import sys


from pyspark import SparkContext, SparkConf
conf = (
    SparkConf()
        .setMaster('local[{}]'.format(8))
        .set('spark.driver.memory', '{}g'.format(12))
)
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 4, 9])
sum_squares = rdd.map(lambda elem: float(elem)**2).sum()
print(sum_squares)
#sc.setLogLevel("INFO")


# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Parallelize (Concurrency, Thread Oriented)

# COMMAND ----------

# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6908168003362015/2853703854010541/4620261684428706/latest.html
# https://spark.apache.org/docs/latest/rdd-programming-guide.html

# import random
# import time
# from pyspark.sql import Row

# def do_work(item):
#   print(f"Work for Job: {item}")
#   delay = random.randint(1,2)
#   time.sleep(delay)
#   result = dict(**item)
#   result.update(delay=delay)
#   return result

# jobinput = [dict(name="job_" + str(y))  for y in range(10)]
# ji_df = sc.parallelize(jobinput, 10)
# result = ji_df.map(do_work).collect()

# # COMMAND ----------



# # COMMAND ----------


