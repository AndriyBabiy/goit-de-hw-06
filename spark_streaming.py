from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from configs import kafka_config
import os
import datetime

# Package for reading Kafka from Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Creating Spark Session
spark = (SparkSession.builder
         .appName('kafkaStream')
        #  .config("spark.executor.memory", "2g")
        #  .config("spark.driver.memory", "2g")
        #  .config("spark.memory.fraction", "0.8")
         .master("local[*]")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# # Створення сесії Spark для обробки даних
# spark = (
#     SparkSession.builder.appName("KafkaStreaming")
#     .master("local[*]")  # Запуск на всіх ядрах локальної машини
#     .config(
#         "spark.sql.debug.maxToStringFields", "200"
#     )  # Максимальна кількість полів для відображення
#     .config(
#         "spark.sql.columnNameLengthThreshold", "200"
#     )  # Максимальна довжина імен стовпців
#     .getOrCreate()  # Створення або отримання існуючої сесії
# )

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
streaming_out =f'{my_name}_spark_streaming_out'

# -----------------------------------------

# # Create Kafka client
# admin_client = KafkaAdminClient(
#   bootstrap_servers=kafka_config['bootstrap_servers'],
#   security_protocol=kafka_config['security_protocol'],
#   sasl_mechanism=kafka_config['sasl_mechanism'],
#   sasl_plain_username=kafka_config['username'],
#   sasl_plain_password=kafka_config['password']
# )

# try:
#   admin_client.create_topics(new_topics=streaming_out, validate_only=False)
#   for topic in topics:
#     print(f"Topic '{topic}' created successfully")
# except Exception as e:
#   print(f"An error occurred: {e}")

# -----------------------------------------
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

# # Завантаження CSV-файлу з умовами для алертів (передумови для порогів температури та вологості)
# alerts_df = spark.read.csv("./data/alerts_conditions.csv", header=True)

# Параметри вікна для обчислень за часом
window_duration = "1 minute"  # Тривалість вікна
sliding_interval = "30 seconds"  # Інтервал зміщення вікна


humidity = 50
temperature = 20

print(alert_conditions_DF.show())

# print(alert_conditions_DF
#       .select("*")
#       .filter(
#         (alert_conditions_DF.humidity_min <= humidity) |
#         (alert_conditions_DF.humidity_max >= humidity) |
#         (alert_conditions_DF.temperature_min <= temperature) |
#         (alert_conditions_DF.temperature_max >= temperature)
#         )
#       .show()
#       )

# # Read stream of data from CSV file
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

# -----------------------------------------

# Schema definition for JSON
json_schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("timestamp", StringType(), True),
  StructField("temperature", IntegerType(), True),
  StructField("humidity", IntegerType(), True),
])

# # Опис структури JSON для декодування вхідних даних
# json_schema = StructType(
#     [
#         StructField("sensor_id", IntegerType(), True),
#         StructField("timestamp", StringType(), True),
#         StructField("temperature", IntegerType(), True),
#         StructField("humidity", IntegerType(), True),
#     ]
# )

# Reading a data stream from Kafka
df = (spark 
  .readStream
  .format('kafka')
  .option('kafka.bootstrap.servers', bootstrap_servers[0])
  .option('kafka.security.protocol', security_protocol)
  .option('kafka.sasl.mechanism', sasl_mechanism)
  .option('kafka.sasl.jaas.config', 
          f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{sasl_plain_username}" password="{sasl_plain_password}";')
  .option('subscribe', building_topic)
  .option('startingOffsets', 'earliest')
  .option('maxOffsetsPerTrigger', '5')
  .load()
  )

# Data manipulation
clean_df = (df
    .selectExpr(
      "CAST(key AS STRING) AS key_deserialized", 
      "CAST(value AS STRING) AS value_deserialized", 
      "*"
    )
    .drop('key', 'value')
    .withColumnRenamed("key_deserialized", "key")
    .withColumn(
      "value_json", from_json(col("value_deserialized"), json_schema)
    )
    .withColumn(
      "timestamp", 
      from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")
    )
    # .withColumn("temperature", col("value_json.temperature"))
    # .withColumn("humidity", col("value_json.humidity"))
    # .drop("value_json", "value_deserialized")
    .withWatermark(
      "timestamp", "10 seconds"
    )
    .groupBy(
      window(col("timestamp"), window_duration, sliding_interval)
    )
    .agg(
      avg("value_json.temperature").alias("t_avg"),
      avg("value_json.humidity").alias("h_avg")
    )
    .drop("topic")
)

# Joining the conditions of the alerts onto the processed incoming data df
all_alerts = clean_df.crossJoin(alert_conditions_DF)

# Filtering and displaying only the alerts that are inside of the alert ranges
valid_alerts = (
  all_alerts.where("t_avg > temperature_min AND t_avg < temperature_max")
  .unionAll(all_alerts.where("h_avg > humidity_min AND h_avg < humidity_max"))
  .withColumn(
    "timestamp", lit(str(datetime.datetime.now()))
  )
  .drop(
    "id", "humidity_min", "humidity_max", "temperature_min", "temperature_max"
  )
)

# Creating a function that will create a unique identifier for each record
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Converting the data to JSON to then be sent to Kafka
prepare_to_kafka_df = valid_alerts.withColumn("key", uuid_udf()).select(
  col("key"),
  to_json(
    struct(
      col("window"),
      col("t_avg"),
      col("h_avg"),
      col("code"),
      col("message"),
      col("timestamp"),
    )
  ).alias(
    "value"
  )
)

# Displaying data to the screen
displaying_df = (valid_alerts.writeStream
                  .trigger(availableNow=True)
                  .outputMode("update")
                  .format('console')
                  .option("checkpointLocation", "/tmp/checkpoints-2")
                  .start()
                  .awaitTermination())

# # Prepare data to push to Kafka
# prepare_to_kafka_df = clean_df.select(
#   col('key'),
#   to_json(struct(col('temperature'), col('humidity'))).alias('value')
# )

# # Wiritng the processed data into the kafka topic 'andriy_b_spark_streaming_out'
# query = (prepare_to_kafka_df.writeStream
#           .trigger(processingTime='1 minute')
#           .format('kafka')
#           .option('kafka.bootstrap.servers', bootstrap_servers)
#           .option('topic', streaming_out)
#           .option('kafka.security.protocol', security_protocol)
#           .option('kafka.sasl.mechanism', sasl_mechanism)
#           .option('kafka.sasl.jaas.config', 
#                   f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{sasl_plain_username}" password="{sasl_plain_password}";')
#           .option('checkpointLocation', '/tmp/checkpoints-3')
#           .start()
#           .awaitTermination())

















