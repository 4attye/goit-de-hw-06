from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_json, struct
from configs import kafka_config
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = SparkSession.builder \
    .appName("KafkaStreamingAlerts") \
    .master("local[*]")\
    .getOrCreate()

raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", f"{kafka_config['my_id']}_building_sensors") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# test_query = df.writeStream.format("console").start()

aggregated_df = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

alerts_conditions = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("alerts_conditions.csv")

final_alerts = aggregated_df.crossJoin(alerts_conditions) \
    .filter(
        # Логіка для температури (використовуємо нові назви temperature_min та temperature_max)
        ((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
        ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max"))) &
        
        # Логіка для вологості (використовуємо нові назви humidity_min та humidity_max)
        ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
        ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max")))
    ) \
    .withColumn("alert_timestamp", current_timestamp()) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "t_avg", 
        "h_avg", 
        "code", 
        "message", 
        "alert_timestamp"
    )

# displaying_df = final_alerts.writeStream \
#     .format("console") \
#     .outputMode("update") \
#     .option("truncate", False) \
#     .start()

query = final_alerts \
    .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", f"{kafka_config['my_id']}_alerts") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("checkpointLocation", "/tmp/checkpoints-1") \
    .outputMode("update") \
    .start()\
    .awaitTermination()