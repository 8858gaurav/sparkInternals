# No of partitions initially.
################### case 1 ########################

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import getpass, time, os
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
           .appName("Partitions") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.conf.get("spark.sql.files.maxPartitionBytes") # '134217728b'
    spark.conf.get("spark.sql.shuffle.partitions") # 200

    # whenerver shuffle happends by default 200 partition will get created.
    # formula for partition size
    # partition_size = min of (maxPartitionBytes, file_size/default parallelism)
    # the file size of lung cancer is 61 MB in hdfs.
    spark.sparkContext.defaultParallelism # default parallelism

    maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes")[0:-1])/1024/1024 # for MB, it's 128
    file_size = os.path.getsize('/home/itv020752/Data/Lung_Cancer.csv')/1024/1024 # for MB
    default_parallelism = spark.sparkContext.defaultParallelism

    b = file_size/default_parallelism #  15.25 mb
    a = maxPartitionBytes # 128 mb

    def minimum(x, y): 
        if x < y:
            return x
        else:
            return y 
    partition_size = minimum(a, b)

    print(a, b)
    # 128.0 15.25

    print("number of partitions", file_size/partition_size)
    # number of partitions 4.0

    # no of partitions = Max(file_size/partition_size, default level of parallelism)

    def maximum(x, y): 
        if x > y:
            return x
        else:
            return y 
    partition_number = maximum(a, b)


    print("No of partitions ideally", maximum(file_size/partition_size,default_parallelism))
    # No of partitions ideally 4

    df = spark.read.format("csv").load("/user/itv020752/data/Lung_Cancer.csv", header = True)

    df.rdd.getNumPartitions() # this is equal to the below one: 4
    print("number of partitions", file_size/partition_size, maximum(file_size/partition_size,default_parallelism))
    # number of partitions 4.0 4

    print(df.count())
    # 4 cores we have, now each core will process to count the no of rows parallely.

    # With this way we can utilize our cluster effectevily without wasting our resources, if we have a more cpu, then we'll get more no of partitions.


    ####################################################################################
    ##### run the same code for around 1 gb file, with 2 level of parallelism ##########

    # No of partitions initially.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .config("spark.dynamicAllocation.enabled", False) \
           .config("spark.executor.instances", 2) \
           .config("spark.executor.cores", 1) \
           .config("spark.executor.memory", '2g') \
           .config('spark.ui.port', '0') \
           .appName("Partitions") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.conf.get("spark.sql.files.maxPartitionBytes") # '134217728b'
    spark.conf.get("spark.sql.shuffle.partitions") # 200

    # whenerver shuffle happends by default 200 partition will get created.
    # formula for partition size
    # partition_size = min of (maxPartitionBytes, file_size/default parallelism)
    # the file size of question tag is 913 MB in locally.
    # the file size of question tag is  MB in hdfs.
    spark.sparkContext.defaultParallelism # default parallelism

    maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes")[0:-1])/1024/1024 # for MB, it's 128
    file_size = os.path.getsize('/home/itv020752/Data/question_tags.csv')/1024/1024 # for MB.
    default_parallelism = spark.sparkContext.defaultParallelism

    b = file_size/default_parallelism # 456.5 mb
    a = maxPartitionBytes # 128 mb

    def minimum(x, y): 
        if x < y:
            return x
        else:
            return y 
    partition_size = minimum(a, b)

    print(a, b) # 456.5, 128 mb

    print("number of partitions", file_size/partition_size)
    # number of partitions 8

    # no of partitions = Max(file_size/partition_size, default level of parallelism)

    print(file_size/partition_size) # 8

    def maximum(x, y): 
        if x > y:
            return x
        else:
            return y 
    partition_number = maximum(a, b)


    print("No of partitions ideally", maximum(file_size/partition_size,default_parallelism))
    # No of partitions ideally 8

    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)

    df.rdd.getNumPartitions() # this is equal to the below one : 8
    print("number of partitions", file_size/partition_size, maximum(file_size/partition_size,default_parallelism))
    # number of partitions 8 8

    print(df.count())
    # 2 cores we have, now each core will process 1 task at a time to count the no of rows parallely.
    # since we have 8 partitons, so each core will 4 task. means 2 task will run parallely at a time.
    # With this way we can utilize our cluster effectevily without wasting our resources, if we have a more cpu, then we'll get more no of partitions.