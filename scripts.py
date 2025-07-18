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
           .appName("weeek# application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')

   spark.sparkContext.getConf().getAll()
# [('spark.eventLog.enabled', 'true'),
#  ('spark.sql.repl.eagerEval.enabled', 'true'),
#  ('spark.eventLog.dir', 'hdfs:///spark-logs'),
#  ('spark.dynamicAllocation.maxExecutors', '10'),
#  ('spark.sql.warehouse.dir', '/user/itv020752/warehouse'),
#  ('spark.yarn.historyServer.address', 'm02.itversity.com:18080'),
#  ('spark.executorEnv.PYTHONPATH',
#   '/opt/spark-3.1.2-bin-hadoop3.2/python/lib/py4j-0.10.9-src.zip:/opt/spark-3.1.2-bin-hadoop3.2/python/<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-0.10.9-src.zip'),
#  ('spark.yarn.jars', ''),
#  ('spark.history.provider',
#   'org.apache.spark.deploy.history.FsHistoryProvider'),
#  ('spark.serializer.objectStreamReset', '100'),
#  ('spark.history.fs.logDirectory', 'hdfs:///spark-logs'),
#  ('spark.submit.deployMode', 'client'),
#  ('spark.history.fs.update.interval', '10s'),
#  ('spark.driver.extraJavaOptions', '-Dderby.system.home=/tmp/derby/'),
#  ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_HOSTS',
#   'm02.itversity.com'),
#  ('spark.ui.filters',
#   'org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter'),
#  ('spark.app.id', 'application_1752426866478_3019'),
#  ('spark.executor.extraLibraryPath', '/opt/hadoop/lib/native'),
#  ('spark.history.ui.port', '18080'),
#  ('spark.shuffle.service.enabled', 'true'),
#  ('spark.driver.appUIAddress', 'http://g01.itversity.com:44881'),
#  ('spark.app.name', 'weeek 1 application'),
#  ('spark.dynamicAllocation.minExecutors', '2'),
#  ('spark.executor.id', 'driver'),
#  ('spark.ui.proxyBase', '/proxy/application_1752426866478_3015'),
#  ('spark.driver.host', 'g01.itversity.com'),
#  ('spark.history.fs.cleaner.enabled', 'true'),
#  ('spark.driver.port', '38811'),
#  ('spark.master', 'yarn'),
#  ('spark.ui.port', '0'),
#  ('spark.sql.catalogImplementation', 'hive'),
#  ('spark.rdd.compress', 'True'),
#  ('spark.app.startTime', '1752834946026'),
#  ('spark.submit.pyFiles', ''),
#  ('spark.yarn.isPython', 'true'),
#  ('spark.dynamicAllocation.enabled', 'true'),
#  ('spark.ui.showConsoleProgress', 'true'),
#  ('spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES',
#   'http://m02.itversity.com:19088/proxy/application_1752426866478_3019')]

# checking execution memory, and storage memory, which is equal to 30 % of your Worker Memory
# say on each worker, we have a 4 executors, and each executor is having 5 CPU core, and 21 GB Ram so 30 % of 84GB Ram = 24GB
# since we have a 20 CPU cores, so each core will get 1.2GB Ram as an execution memory, & 1.2GB will get as a storage memory. 

# Ideally each executor is having 1 GB Ram, and 1 CPU Cores

# check this from the enviroment tab with the help of spark UI
# Executor Reqs:
# 	cores: [amount: 1]
# 	memory: [amount: 1024]
# 	offHeap: [amount: 0]
# Task Reqs:
# 	cpus: [amount: 1.0]

# how many containers you have for your running spark applications, check this from the executor tab with the help of spark UI

# Executor ID	       Address	                          Status	            RDD Blocks	         Storage Memory
# driver	        g01.itversity.com:44177	              Active	              0	                 34.8 KiB / 397.5 MiB
# 1	            w02.itversity.com:45925	                  Active	              0	                 34.8 KiB / 366.3 MiB
# 2	            w03.itversity.com:45635	                  Active	              0	                  0.0 B / 366.3 MiB

# [itv020752@g01 ~]$ yarn node -list -all
# 2025-07-18 05:38:21,665 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at m02.itversity.com/172.16.1.104:8032
# 2025-07-18 05:38:21,782 INFO client.AHSProxy: Connecting to Application History server at m01.itversity.com/172.16.1.103:10200
# Total Nodes:3
#          Node-Id             Node-State Node-Http-Address       Number-of-Running-Containers
# w02.itversity.com:37065         RUNNING w02.itversity.com:8042                            17
# w03.itversity.com:34105         RUNNING w03.itversity.com:8042                            17
# w01.itversity.com:33329       UNHEALTHY w01.itversity.com:8042  

df = spark.read.format("csv").load("/user/itv020752/data/question_tags.csv", header = True)

# since we have a 1GB Ram as a worker node in this case, so 30% of 1 GB ~= 366.3 MB as a execution memory, 
# this execution memory is responsible for handling the task in our container/executor. So our container CPU, which is 1 CPU, can handle 
# max to max 366.3 M of data for processing. 

# we have a 1GB Ram as a worker node in this case, so 30% of 1 GB ~= 366.3 MB as a storage memory, 
# this storage memory is responsible for storing the data in our container/executor. So our container CPU, which is 1 CPU, can handle 
# max to max 366.3 M of data for storage.