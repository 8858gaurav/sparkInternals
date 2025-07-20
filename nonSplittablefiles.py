# for non-splittable files, when we read the gzip, and snappy files, it this files were written in a csv format. 
# even though our default min partitions = 2, even though our default parallelism = 2
# we can't get the benefits of parallelism, if we split this files (gzip, snappy), then our files will become corrupt.

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
           .config('spark.ui.port', '0') \
           .appName("Partitions") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.conf.get("spark.sql.files.maxPartitionBytes") # '134217728b'
    spark.conf.get("spark.sql.shuffle.partitions") # 200
    
    spark.conf.get("spark.dynamicAllocation.enabled")
#     spark.conf.get("spark.executor.instances")
#     spark.conf.get("spark.executor.core")
#     spark.conf.get("spark.executor.memory")

    # whenerver shuffle happends by default 200 partition will get created.
    # formula for partition size
    # partition_size = min of (maxPartitionBytes, file_size/default parallelism)
    # no of partitions = Max(file_size/partition_size, default level of parallelism)
    # the file size of question tag is 503 MB in locally.
    # the file size of question tag is  503 MB in hdfs.
    
    maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes")[0:-1])/1024/1024 # for MB, it's 128
    file_size = os.path.getsize('/home/itv020752/Data/question_tags.csv')/1024/1024 # for MB.
    default_parallelism = spark.sparkContext.defaultParallelism

    print(maxPartitionBytes, file_size, default_parallelism) #. 128.0 503.0 2

    df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)

    df.rdd.getNumPartitions() # this is equal to the below one : 4
    # file_size/partition_size or Max(file_size/partition_size, default level of parallelism)
    
    # if I write the data back to disk, I'll get 4 files, since we have a 4 partitions.
    
    new_df = df.repartition(1) # to write it as a single file
    new_df.write.format('csv').mode("overwrite").option("codec", "gzip").save("new_df_gz")
    # for snappy, used .option("codec", "snappy"). Snappy take higher space than gzip on any storage.
    # for the same coode, snappy would create a file, whose length is 270 MB.
    
    # !hadoop fs -ls -h new_df_gz # gives you 1 files, size is 139.5 MB

    # Found 2 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-07-20 03:30 new_df_gz/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    139.5 M 2025-07-20 03:30 new_df_gz/part-00000-64b344bf-c4bb-4707-a559-04e86b4e9086-c000.csv.gz

    # hadoop fs -head new_df_gz/part-00000-64b344bf-c4bb-4707-a559-04e86b4e9086-c000.csv.gz
    # the output of the above files is in encrypted format, humans can't read it. 
    
    new_df_gz = spark.read.format("csv").load("new_df_gz")
    new_df_gz.rdd.getNumPartitions() # 1, why 1, because our file is not splittable, if we split this file, we'll get an error.
    # even though this data is > 128 MB.
    
    new_df_gz.repartition(4).write.format('csv').mode("overwrite").save("df_gz_after_splitting")

    # !hadoop fs -ls -h df_gz_after_splitting
    # Found 5 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-07-20 03:46 df_gz_after_splitting/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    125.8 M 2025-07-20 03:44 df_gz_after_splitting/part-00000-485596b7-27fa-4a21-bb17-119cc56193a5-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    125.8 M 2025-07-20 03:46 df_gz_after_splitting/part-00001-485596b7-27fa-4a21-bb17-119cc56193a5-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    125.7 M 2025-07-20 03:45 df_gz_after_splitting/part-00002-485596b7-27fa-4a21-bb17-119cc56193a5-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    125.7 M 2025-07-20 03:46 df_gz_after_splitting/part-00003-485596b7-27fa-4a21-bb17-119cc56193a5-c000.csv

    df_gz_after_splitting = spark.read.format("csv").load("df_gz_after_splitting")
    df_gz_after_splitting.show()
    # +--------+-----------------+
    # |     _c0|              _c1|
    # +--------+-----------------+
    # |26566160|             java|
    # |20723488|       javascript|
    # |21338771|             ajax|
    # | 5049531|              php|
    # | 1043172|            forms|
    # |19612915|       javascript|
    # |20518376|                r|
    # |13287053|               c#|
    # |24908837|          jscript|
    # | 5897148|            video|
    # | 1302479|               c#|
    # | 4257074|          dbghelp|
    # |23599563|               c#|
    # |25453601|        doctrine2|
    # |17758349|       navigation|
    # |23309884|          testing|
    # |17174950|            linux|
    # |23610559|            cloud|
    # |15421303|hibernate-mapping|
    # |21682771|             html|
    # +--------+-----------------+

    df_gz_after_splitting.count() # 30438393

    df_gz_after_splitting.rdd.getNumPartitions() # 4
        