import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import getpass, time
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .config('spark.ui.port', '0') \
           .appName("application3") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # spark.dynamicAllocation.enabled	True: by default it's a True.
    # spark.dynamicAllocation.minExecutors	2
     # spark.dynamicAllocation.maxExecutors	10
    spark.sparkContext.defaultParallelism # we have 1 cpu core, and 2 executors. that's why are we getting 2 level of parallelism at the starting point by default.
    # because spark.sparkContext.defaultMinPartitions = 2
    
    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)
    spark.sql("select count(*) from df_view").show()
    # in the executor tab, we'll be able to see 1 cores for each executors for this job. No of executor = 2

    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI after runnng the above code.

    # Executor ID	       Address	                          Status	            RDD Blocks	         Storage Memory
    # driver	        g01.itversity.com:44177	              Active	              0	                 0.0 B / 397.5 MiB
    # 1	            w02.itversity.com:45925	                  Active	              0	                 0.0 B / 366.3 MiB
    # 2	            w03.itversity.com:45635	                  Active	              0	                 0.0 B / 366.3 MiB


    # check this from the enviroment tab with the help of spark UI
    # Executor Reqs:
    #       cores: [amount: 1]
    #       memory: [amount: 1024]
    #       offHeap: [amount: 0]
    # Task Reqs:
    #       cpus: [amount: 1.0]


    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI after runnng the above code.

    # Executor ID	       Address	                          Status	            RDD Blocks	diskUsed     Storage Memory            cores
    # driver	        g01.itversity.com:44177	              Active	              0	          0B       107.5 KiB / 397.5 MiB        0
    # 1	            w02.itversity.com:45925	                  Active	              0	          0B       77.7 KiB / 366.3 MiB          2
    # 2	            w03.itversity.com:45635	                  Active	              0	          0B       37.5 KiB / 366.3 MiB	          2
                                                                                                                
    # Now save this file locally on your system, run this commands
    # 
    # spark3-submit --master yarn \
    # --num-executors 2 \
    # --executor-cores 1 \
    # --executor-memory 1g \
    # --conf "spark.dynamic.Allocation.enabled=false" \
    # scripts3.py                                                                                                           