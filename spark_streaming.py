from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from configs import kafka_config
import os

# Package for reading Kafka from Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Creating Spark Session
spark = (SparkSession.builder
         .appName('csvStream')
         .master("local[*]")
         .getOrCreate())

# -----------------------------------------
# Config components
bootstrap_servers=kafka_config['bootstrap_servers']
security_protocol=kafka_config['security_protocol']
sasl_mechanism=kafka_config['sasl_mechanism']
sasl_plain_username=kafka_config['username']
sasl_plain_password=kafka_config['password']

# Topic name
my_name='andriy_b'
building_topic=f'{my_name}_building_sensors'
humidity_topic=f'{my_name}_humidity_alert'
temperature_topic=f'{my_name}_temperature_alert'
# -----------------------------------------

# Reading a data stream from Kafka
df = spark \
  .readStream \
  .format('kafka') \
  .option('kafka.bootstrap.servers', bootstrap_servers) \
  .option('kafka.security.protocol', security_protocol) \
  .option('kafka.sasl.jaas.config', 
          f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{sasl_plain_username}" password="{sasl_plain_password}";') \
  .option('subscribe', building_topic) \
  .option('startingOffsets', 'earliest') \
  .option('maxOffsetsPerTrigger', ) \
  .load()

# Schema definition for JSON
json_schema = StructType([
  StructField("timestamp", StringType(), True),
  StructField("temperature", IntegerType(), True),
  StructField("humidity", IntegerType(), True),
])

# Data manipulation
clean_df = df.selectExpr('*')

# Displaying data to the screen
displaying_df = clean_df.writeStream \
  .trigger(availableNow=True) \
  .outputMode("append") \
  .format('console') \
  .option("checkpointLocation", "/tmp/checkpoints-2") \
  .start() \
  .awaitTermination()


# Outlining the CSV schema
schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("humidity_min", IntegerType(), True),
  StructField("humidity_max", IntegerType(), True),
  StructField("temperature_min", IntegerType(), True),
  StructField("temperature_max", IntegerType(), True),
  StructField("code", IntegerType(), True),
  StructField("message", StringType(), True)
])

# Read conditional data from csv file
alert_conditions_DF = spark.read.csv('./data/alerts_conditions.csv', header=True)
alert_conditions_DF.createTempView('alert_conditions_view')

humidity = 50
temperature = 20

# print(alert_conditions_DF.show())

print(alert_conditions_DF
      .select("*")
      .filter(
        (alert_conditions_DF.humidity_min <= humidity) |
        (alert_conditions_DF.humidity_max >= humidity) |
        (alert_conditions_DF.temperature_min <= temperature) |
        (alert_conditions_DF.temperature_max >= temperature)
        )
      .show()
      )

# Read stream of data from CSV file
# alert_conditionsDF = spark.readStream \
#   .option("sep", ",") \
#   .option("header", True) \
#   .option("maxFilesPerTrigger", 1) \
#   .schema(schema) \
#   .csv('./data')

# query = (alert_conditionsDF.writeStream
#          .trigger(availableNow=True)
#          .outputMode("append")
#          .format("console")
#          .start()
#          )

# query.awaitTermination()



