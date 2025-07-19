# how & when the partiton size of the data will changes from 128mb to other. 
# How partition size will change, or how the number of partitions will change.
# ideally, if we load the data from the HDFS, then partition size = block size

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
           .config("spark.sql.files.maxPartitionBytes", "134217728") \
           .config("spark.sql.shuffle.partitions", "200") \
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

    # whenerver shuffle happends by default 200 partition will get created.
    # formula for partition size
    # partition_size = min of (maxPartitionBytes, file_size/default parallelism)
    # the file size of lung cancer is 61 MB in hdfs.
    spark.sparkContext.defaultParallelism # default parallelism

    maxPartitionBytes = 134217728/1024/1024 # for MB
    file_size = os.path.getsize('/home/itv020752/Data/Lung_Cancer.csv')/1024/1024 # for MB, it's 128
    default_parallelism = spark.sparkContext.defaultParallelism

    b = file_size/default_parallelism
    a = maxPartitionBytes

    def minimum(x, y): 
        if x < y:
            return x
        else:
            return y 
    partition_size = minimum(a, b)

    print(a, b)

    print("number of partitions", file_size/partition_size)

    spark.sparkContext.getConf().getAll()
    # [('spark.eventLog.enabled', 'true'),
    #  ('spark.sql.repl.eagerEval.enabled', 'true'),
    #  ('spark.eventLog.dir', 'hdfs:///spark-logs'),
    #  ('spark.dynamicAllocation.maxExecutors', '10'),
    #  ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES',
    #   'http://m02.itversity.com:19088/proxy/application_1752426866478_3598'),
    #  ('spark.app.startTime', '1752933533854'),
    #  ('spark.driver.appUIAddress', 'http://g01.itversity.com:44709'),
    #  ('spark.sql.warehouse.dir', '/user/itv020752/warehouse'),
    #  ('spark.app.id', 'application_1752426866478_3598'),
    #  ('spark.yarn.historyServer.address', 'm02.itversity.com:18080'),
    #  ('spark.executorEnv.PYTHONPATH',
    #   '/opt/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip:/opt/spark-3.1.2-bin-hadoop3.2/python/<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9-src.zip'),
    #  ('spark.yarn.jars', ''),
    #  ('spark.driver.port', '41057'),
    #  ('spark.history.provider',
    #   'org.apache.spark.deploy.history.FsHistoryProvider'),
    #  ('spark.executor.instances', '2'),
    #  ('spark.serializer.objectStreamReset', '100'),
    #  ('spark.sql.shuffle.partitions', '200'),
    #  ('spark.history.fs.logDirectory', 'hdfs:///spark-logs'),
    #  ('spark.submit.deployMode', 'client'),
    #  ('spark.history.fs.update.interval', '10s'),
    #  ('spark.driver.extraJavaOptions', '-Dderby.system.home=/tmp/derby/'),
    #  ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS',
    #   'm02.itversity.com'),
    #  ('spark.ui.filters',
    #   'org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter'),
    #  ('spark.executor.extraLibraryPath', '/opt/hadoop/lib/native'),
    #  ('spark.executor.memory', '2g'),
    #  ('spark.history.ui.port', '18080'),
    #  ('spark.shuffle.service.enabled', 'true'),
    #  ('spark.ui.proxyBase', '/proxy/application_1752426866478_3584'),
    #  ('spark.dynamicAllocation.minExecutors', '2'),
    #  ('spark.executor.id', 'driver'),
    #  ('spark.executor.cores', '2'),
    #  ('spark.driver.host', 'g01.itversity.com'),
    #  ('spark.history.fs.cleaner.enabled', 'true'),
    #  ('spark.dynamicAllocation.enabled', 'False'),
    #  ('spark.app.name', 'application'),
    #  ('spark.master', 'yarn'),
    #  ('spark.ui.port', '0'),
    #  ('spark.sql.catalogImplementation', 'hive'),
    #  ('spark.rdd.compress', 'True'),
    #  ('spark.submit.pyFiles', ''),
    #  ('spark.yarn.isPython', 'true'),
    #  ('spark.sql.files.maxPartitionBytes', '134217728'),
    #  ('spark.ui.showConsoleProgress', 'true')]


    df = spark.read.format("csv").load("/user/itv020752/data/Lung_Cancer.csv", header = True)

    df.rdd.getNumPartitions() # this is equal to the below one
    print("number of partitions", file_size/partition_size)

    print(df.count())
    # 4 cores we have, now each core will process to count the no of rows parallely.