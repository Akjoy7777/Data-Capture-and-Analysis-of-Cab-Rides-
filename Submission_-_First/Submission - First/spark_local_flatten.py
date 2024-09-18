# Importing required libraries
import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#Initializing Spark session/application
spark=SparkSession.builder.appName("Kafka-to-HDFS").master("local").getOrCreate()
spark

#Reading data from hdfs
df=spark.read.json("/user/root/clickstream_data_dump/part-00000-3865fbe8-b81c-4ba6-ba1c-2175a0868351-c000.json")


df.show(10,truncate=False)

#Selecting the columns from the clickstream data set and assigning alias
df=df.select(get_json_object(df['value_str'],"$.customer_id").alias("customer_id"),
            get_json_object(df['value_str'],"$.app_version").alias("app_version"),
            get_json_object(df['value_str'],"$.OS_version").alias("OS_version"),
            get_json_object(df['value_str'],"$.lat").alias("lat"),
            get_json_object(df['value_str'],"$.lon").alias("lon"),
            get_json_object(df['value_str'],"$.page_id").alias("page_id"),
            get_json_object(df['value_str'],"$.button_id").alias("button_id"),
            get_json_object(df['value_str'],"$.is_button_click").alias("is_button_click"),
            get_json_object(df['value_str'],"$.is_page_view").alias("is_page_view"),
            get_json_object(df['value_str'],"$.is_scroll_up").alias("is_scroll_up"),
            get_json_object(df['value_str'],"$.is_scroll_down").alias("is_scroll_down"),
            get_json_object(df['value_str'],"$.timestamp").alias("timestamp")
             )
# Filter out rows where customer_id is not null or empty
df = df.filter(col("customer_id").isNotNull() & (col("customer_id") != ""))

#validating schema
df.printSchema()

#Validating records in top 10 records
df.show(10)

#Writing the format final dataset to hdfs
df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/clickstream_flattened',header='true')