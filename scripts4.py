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
           .appName("application4") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # spark.dynamicAllocation.enabled	True: by default it's a True.
    # spark.dynamicAllocation.minExecutors	2
     # spark.dynamicAllocation.maxExecutors	10
    spark.sparkContext.defaultParallelism # 12 
    
    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)
    spark.sql("select count(*) from df_view").show()
    # in the executor tab, we'll be able to see 4 cores for each executors for this job. No of executor = 3


    # check this from the enviroment tab with the help of spark UI
    # Executor Reqs:
    #       cores: [amount: 1]
    #       memory: [amount: 1024]
    #       offHeap: [amount: 0]
    # Task Reqs:
    #       cpus: [amount: 1.0]


    # how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI after runnng the above code.

    # Executor ID	       Address	                          Status	            RDD Blocks	diskUsed     Storage Memory            cores
    # driver	        g01.itversity.com:44177	              Active	              0	          0B       107.5 KiB / 397.5 MiB         0
    # 1	            w02.itversity.com:45925	                  Active	              0	          0B       77.7 KiB / 366.3 MiB          4
    # 2	            w03.itversity.com:45635	                  Active	              0	          0B       37.5 KiB / 366.3 MiB	         4
    # 3             w01.itversity.com:45635                   Active                  0           0B       37.5 KiB / 366.3 MiB          4
                                                                                                                
    # Now save this file locally on your system, run this commands
    # driver runs in one of the executor with in the cluster. we can't see the output, since our driver is running in the cluster mode.
    # 12 task will run parallel at first go, why 3 (executors) * 4 (cores)

    # spark3-submit \
    # --master yarn \
    #--deploy-mode cluster \ 
    # --num-executors 3 \
    # --executor-cores 4 \
    # --executor-memory 2g \ 
    # --conf "spark.dynamicAllocation.enabled=false" \
    # scripts4.py                                                                                                           