# disabling the dynamic allocation enabled. 

########################################
## case 1 ##############################

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
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.executor.instances", 2) \
           .config("spark.executor.cores", 2) \
           .config("spark.executor.memory", '2g') \
           .config('spark.ui.port', '0') \
           .appName("application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.sparkContext.defaultParallelism # since we have a 4 core at the startion point of the cluster. At the starting point, we can run 4 task in para;;el.
    # 2 (Cores) * 2 (executors) = 4, each executor is having 2 cores. 

    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)

    df.createOrReplaceTempView("df_view")

    spark.sql("select count(*) from df_view").show()
    # in the executor tab, we'll be able to see 2 cores for each executors for this job.

    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI

    # Executor ID	       Address	                          Status	            RDD Blocks	diskUsed     Storage Memory            cores
    # driver	        g01.itversity.com:44177	              Active	              0	          0B       107.9 KiB / 397.5 MiB        0
    # 1	            w02.itversity.com:45925	                  Active	              0	          0B      37.5 KiB / 912.3 MiB          2
    # 2	            w03.itversity.com:45635	                  Active	              0	          0B     77.9 KiB / 912.3 MiB           2


    # check this from the enviroment tab with the help of spark UI
    # Executor Reqs:
    # 	cores: [amount: 2]
    # 	memory: [amount: 2048]
    # 	offHeap: [amount: 0]
    # Task Reqs:
    # 	cpus: [amount: 1.0]

####################################################
######## case 2 ####################################

# Now change from 2 to 1. 

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
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.executor.instances", 1) \
           .config("spark.executor.cores", 1) \
           .config("spark.executor.memory", '1g') \
           .config('spark.ui.port', '0') \
           .appName("2application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # check this from the enviroment tab with the help of spark UI
    # Executor Reqs:
    # 	cores: [amount: 1]
    # 	memory: [amount: 1024]
    # 	offHeap: [amount: 0]
    # Task Reqs:
    # 	cpus: [amount: 1.0]

    spark.sparkContext.defaultParallelism # although we have 1 cpu core, then why are we getting 2 level of parallelism at the starting point.
    # because spark.sparkContext.defaultMinPartitions = 2

    # in the executor tab, we'll be able to see 1 cores for 1 executors for this job.

    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI

    # Executor ID	       Address	                          Status	            RDD Blocks	diskUsed     Storage Memory            cores
    # driver	        g01.itversity.com:44177	              Active	              0	          0B       0.0 B / 397.5 MiB            0
    # 1	            w02.itversity.com:45925	                  Active	              0	          0B      0.0 B / 366.3 MiB             1

    # here storage memory = 30 % of 1GB (WN) = 366.3 MB
    # here execution memory = 30 % of 1 GB (WN) = 366.3 MB

    # our 1 cpu core can handle/processk tas up to 366.3MB
    # our 1 cpu core can store the data up to 366.3 MB, if your data is more than 366.3 MB, then it will spill rest of the data to the disk, which is diskUsed.

    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)
    df.createOrReplaceTempView("df_view")

    spark.sql("select count(*) from df_view").show()

    df.rdd.getNumPartitions() # 2, these 2 task run one by one under the same 1 cpu core, these 2 task will not run in parallel.

    # after runnning the above code, you'll get something like this: 

    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI

    # Executor ID	       Address	                          Status	            RDD Blocks	diskUsed     Storage Memory            cores
    # driver	        g01.itversity.com:44177	              Active	              0	          0B      137.4 KiB / 397.5 MiB            0
    # 1	            w02.itversity.com:45925	                  Active	              0	          0B      77.8 KiB / 366.3 MiB             1

