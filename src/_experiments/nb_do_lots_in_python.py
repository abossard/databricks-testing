import random
import time
from pyspark.sql import Row
from pyspark import SparkContext
import asyncio

def step1():
  async def do_work_async(jobnumber):
    print(f"Start Job {jobnumber}")
    await asyncio.sleep(1)
    print(f"End Job {jobnumber}")

  async def async_main():
    tasks = [do_work_async(y) for y in range(10000)]
    await asyncio.gather(*tasks)

  import time
  s = time.perf_counter()
  asyncio.run(async_main())
  elapsed = time.perf_counter() - s
  print(f"executed in {elapsed:0.2f} seconds.")

def step2():
  def do_work(item):
    print(f"Work for Job: {item}")
    delay = random.randint(1,2)
    time.sleep(delay)
    result = dict(**item)
    result.update(delay=delay)
    return result

  jobinput = [dict(name="job_" + str(y))  for y in range(10)]
  ji_df = sc.parallelize(jobinput, 10)
  return ji_df.map(do_work).collect()

# COMMAND ----------



# COMMAND ----------



if __name__ == "__main__":
    #spark = SparkSession.builder.getOrCreate()
    #sc = spark.sparkContext
    
    sc = SparkContext(master='local[2]', appName=__name__)
    step1()
    print(step2())
    
    
