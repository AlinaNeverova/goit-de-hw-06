from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, to_json, struct, avg, window, to_timestamp,
                                        lit, current_timestamp, round as spark_round)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config, building_sensors_topic, alerts_topic, alerts_file_path


# SparkSession з підтримкою Kafka
spark = (
    SparkSession.builder
    .appName("IoTAlertsStreaming")
    .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# схема повідомлення, яке приходить від sensor_producer
sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Зчитуємо .csv з умовами алертів
alerts_conditions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(alerts_file_path)
)

# Зчитуємо потік даних із Kafka
raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
    .option("subscribe", building_sensors_topic)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .load()
)

# Перетворюємо Kafka value з bytes у json структуру
parsed_df = (
    raw_kafka_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), sensor_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

# Обчислюємо середні значення за sliding window
aggregated_df = (
    parsed_df
    .withWatermark("event_time", "10 seconds")
    .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )
)

# Робимо cross join із таблицею умов алертів
joined_df = aggregated_df.crossJoin(alerts_conditions_df)

# Логіка фільтрації
alerts_df = (
    joined_df
    .filter(
        ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
        ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max"))) &
        ((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
        ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max")))
    )
    .select(
        struct(
            struct(
                col("window.start").alias("start"),
                col("window.end").alias("end")
            ).alias("window"),
            spark_round(col("t_avg"), 2).alias("t_avg"),
            spark_round(col("h_avg"), 2).alias("h_avg"),
            col("code").cast("string").alias("code"),
            col("message").alias("message"),
            current_timestamp().cast("string").alias("timestamp")
        ).alias("value")
    )
)

# вивід алертів в консоль
console_query = (
    alerts_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    #.option("numRows", 10)
    .option("checkpointLocation", "checkpoints/console_alerts")
    .start()
)

# Записуємо алерти назад у Kafka топік
kafka_query = (
    alerts_df
    .selectExpr("to_json(value) as value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
    .option("topic", alerts_topic)
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .option("checkpointLocation", "checkpoints/kafka_alerts")
    .start()
)

# Утримуємо стрім активним
spark.streams.awaitAnyTermination()