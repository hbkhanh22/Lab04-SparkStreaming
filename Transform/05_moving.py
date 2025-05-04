from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, stddev, window, from_json, to_timestamp, collect_list, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

class BTC_Price_Moving:
    def __init__(self, 
                 kafka_bootstrap_servers: str, 
                 input_topic: str,
                 output_topic: str,
                 checkpoint_dir: str) -> None:
        """
        Initialize the BTC_Price_Moving class with Kafka parameters and Spark session.
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = input_topic
        self.intermediate_topic = input_topic + "-window-stats"
        self.output_topic = output_topic
        self.checkpoint_dir_1 = os.path.join(checkpoint_dir, "moving_window")
        self.checkpoint_dir_2 = os.path.join(checkpoint_dir, "moving_grouped")
        
        # Correct window durations as per requirements
        self.windows = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]

        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("btc-price-transformer") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        
        # Ensure checkpoint directories exist (local filesystem)
        os.makedirs(self.checkpoint_dir_1.replace("file://", ""), exist_ok=True)
        os.makedirs(self.checkpoint_dir_2.replace("file://", ""), exist_ok=True)
        
        # Define schema for raw and intermediate streams
        self.raw_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        self.inter_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("symbol", StringType(), True),
            StructField("window", StringType(), True),
            StructField("avg_price", DoubleType(), True),
            StructField("std_price", DoubleType(), True)
        ])

    def read_streaming_data(self) -> DataFrame:
        """
        Read streaming data from the Kafka topic.
        """
        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Extract the JSON payload from 'value'
        parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.raw_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
            .withWatermark("timestamp", "10 seconds")
        return parsed_df

    def compute_stats_for_window(self, df: DataFrame, window_duration: str) -> DataFrame:
        """
        Compute statistics for the given window duration.
        
        Returns a DataFrame with columns: window, symbol, avg_price, std_price
        """
        abbr = {
            "30 seconds": "30s",
            "1 minute": "1m",
            "5 minutes": "5m",
            "15 minutes": "15m",
            "30 minutes": "30m",
            "1 hour": "1h"
        }.get(window_duration, window_duration)

        return df.groupBy(
                window(col("timestamp"), window_duration).alias("win"),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price")
            ) \
            .select(
                col("win.end").alias("timestamp"),
                col("symbol"),
                lit(abbr).alias("window"),
                col("avg_price"),
                col("std_price")
            )

    def write_window_stats(self, df: DataFrame) -> None:
        # write intermediate stats
        kafka_df = df.selectExpr(
            "CAST(symbol AS STRING) as key",
            "to_json(struct(*)) AS value"
        )
        kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", self.intermediate_topic) \
            .option("checkpointLocation", self.checkpoint_dir_1) \
            .outputMode("append") \
            .start()

    def read_window_stats(self) -> DataFrame:
        interm = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.intermediate_topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.inter_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "10 seconds")

        return interm.groupBy("timestamp", "symbol") \
            .agg(collect_list(struct("window", "avg_price", "std_price")).alias("windows"))

    def write_to_kafka(self, df: DataFrame) -> DataFrame:
        """
        Write the processed DataFrame to Kafka.
        """
        kafka_df = df.selectExpr(
            "CAST(symbol AS STRING) AS key",
            "to_json(struct(timestamp, symbol, windows)) AS value"
        )
        kafka_query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", self.checkpoint_dir_2) \
            .outputMode("append") \
            .start()
        return kafka_query
        
    def run(self):
        """
        Run the streaming job.
        """
        # For debugging
        print("Starting BTC Price Moving Average app...")
        
        # Read streaming data from Kafka
        df = self.read_streaming_data()
        
        # For debugging
        print("Input schema:")
        df.printSchema()
        
        stats = None
        for w in self.windows:
            win_df = self.compute_stats_for_window(df, w)
            stats = win_df if stats is None else stats.unionByName(win_df)
        self.write_window_stats(stats)

        # Read back and group
        grouped = self.read_window_stats()
        self.write_to_kafka(grouped)

        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    checkpoint_path = "file://" + os.path.abspath("../spark_checkpoints/btc-price-moving")
    app = BTC_Price_Moving(
        kafka_bootstrap_servers="localhost:9092",
        input_topic="btc-price",
        output_topic="btc-price-moving",
        checkpoint_dir=checkpoint_path
    )
    app.run()