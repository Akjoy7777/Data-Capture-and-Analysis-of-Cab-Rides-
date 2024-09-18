#Importing required libraries
import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Initializing Spark session/application
spark = SparkSession \
       .builder \
       .appName("Kafka-to-local") \
       .getOrCreate()

#Reading Streaming data from de-capstone3 kafka topic
df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
       .option("startingOffsets", "earliest") \
       .option("subscribe", "de-capstone3") \
       .load()

df= df \
      .withColumn('value_str',df['value'].cast('string').alias('key_str')).drop('value') \
      .drop('key','topic','partition','offset','timestamp','timestampType')

#Writing data from kakfa to local file
df.writeStream \
  .format("json") \
  .outputMode("append") \
  .option("path", "/user/root/clickstream_data_dump") \
  .option("checkpointLocation", "/user/root/clickstream_data_dump_cp") \
  .start() \
  .awaitTermination()
