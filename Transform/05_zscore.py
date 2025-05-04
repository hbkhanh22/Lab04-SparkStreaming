from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, from_json, to_timestamp, struct, collect_list, when, expr, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
import os

class BTC_Price_ZScore:
    def __init__(self, 
                 kafka_bootstrap_servers: str, 
                 input_topic_price: str,
                 input_topic_moving: str,
                 output_topic_zscore: str,
                 checkpoint_dir: str) -> None:
        """
        Initialize the BTC_Price_ZScore class with Kafka parameters and Spark session.
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic_price = input_topic_price
        self.input_topic_moving = input_topic_moving
        self.output_topic_zscore = output_topic_zscore
        self.checkpoint_dir = os.path.join(checkpoint_dir, "zscore")
        
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("btc-price-zscore") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        
        # Ensure checkpoint directory exists (local filesystem)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Define schema for price data (from btc-price topic)
        self.price_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        # Define schema for moving statistics data (from btc-price-moving topic)
        self.moving_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("symbol", StringType(), True),
            StructField("windows", ArrayType(StructType([
                StructField("window", StringType(), True),
                StructField("avg_price", DoubleType(), True),
                StructField("std_price", DoubleType(), True)
            ])), True)
        ])

    def read_streaming_data_price(self) -> DataFrame:
        """
        Read price streaming data from Kafka.
        """
        price_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.input_topic_price) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.price_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")) \
            .withWatermark("timestamp", "10 seconds") \
            .filter(col("price").isNotNull() & col("timestamp").isNotNull())  # Handle null prices/timestamps

        # Debug output to console
        # price_df.writeStream \
        #     .format("console") \
        #     .outputMode("append") \
        #     .option("truncate", False) \
        #     .start()
        
        return price_df

    def read_streaming_data_moving(self) -> DataFrame:
        """
        Read moving statistics streaming data from Kafka.
        """
        moving_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.input_topic_moving) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.moving_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "10 seconds") \
            .filter(col("windows").isNotNull())  # Ensure windows array is not null

        # Debug output to console
        # moving_df.writeStream \
        #     .format("console") \
        #     .outputMode("append") \
        #     .option("truncate", False) \
        #     .start()
        
        return moving_df

    def compute_zscore(self, price_df: DataFrame, moving_df: DataFrame) -> DataFrame:
        """
        Compute Z-score by joining price and moving data for each window.
        """
        # Explode the windows array to process each window individually
        moving_exploded = moving_df.select(
            col("timestamp"),
            col("symbol"),
            explode(col("windows")).alias("window_stats")
        ).select(
            col("timestamp"),
            col("symbol"),
            col("window_stats.window").alias("window"),
            col("window_stats.avg_price").alias("avg_price"),
            col("window_stats.std_price").alias("std_price"),
            # Compute window duration in seconds for join condition
            when(col("window_stats.window") == "30s", 30)
                .when(col("window_stats.window") == "1m", 60)
                .when(col("window_stats.window") == "5m", 300)
                .when(col("window_stats.window") == "15m", 900)
                .when(col("window_stats.window") == "30m", 1800)
                .when(col("window_stats.window") == "1h", 3600)
                .otherwise(0).alias("window_seconds")
        )

        # Join price and moving data based on timestamp containment
        joined_df = price_df.alias("p").join(
            moving_exploded.alias("m"),
            (col("p.symbol") == col("m.symbol")) &
            (col("p.timestamp") >= col("m.timestamp") - (col("m.window_seconds") * expr("interval 1 second"))) &
            (col("p.timestamp") < col("m.timestamp")),
            "inner"
        )

        # Compute Z-score, handling edge cases
        zscore_df = joined_df.select(
            col("p.timestamp").alias("timestamp"),
            col("p.symbol").alias("symbol"),
            col("m.window").alias("window"),
            when(
                (col("m.std_price").isNotNull()) & (col("m.std_price") > 0),
                (col("p.price") - col("m.avg_price")) / col("m.std_price")
            ).otherwise(None).alias("zscore_price")
        )

        # Group by timestamp and symbol, collect Z-scores into an array
        final_df = zscore_df.groupBy("timestamp", "symbol").agg(
            collect_list(struct("window", "zscore_price")).alias("zscores")
        )

        return final_df

    def write_to_kafka(self, df: DataFrame) -> DataFrame:
        """
        Write the resulting Z-scores DataFrame to Kafka.
        """
        kafka_df = df.selectExpr(
            "CAST(symbol AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        kafka_query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", self.output_topic_zscore) \
            .option("checkpointLocation", self.checkpoint_dir) \
            .outputMode("append") \
            .start()

        # Debug output to console
        console_query = df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", False) \
            .start()

        return kafka_query

    def run(self):
        """
        Run the streaming job.
        """
        print("Starting BTC Price Z-Score app...")
        
        price_df = self.read_streaming_data_price()
        print("Price DataFrame schema:")
        price_df.printSchema()
        
        moving_df = self.read_streaming_data_moving()
        print("Moving DataFrame schema:")
        moving_df.printSchema()
        
        zscore_df = self.compute_zscore(price_df, moving_df)
        print("Z-Score DataFrame schema:")
        zscore_df.printSchema()
        
        query = self.write_to_kafka(zscore_df)
        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    checkpoint_path = "file://" + os.path.abspath("../spark_checkpoints")
    app = BTC_Price_ZScore(
        kafka_bootstrap_servers="localhost:9092",
        input_topic_price="btc-price",
        input_topic_moving="btc-price-moving",
        output_topic_zscore="btc-price-zscore",
        checkpoint_dir=checkpoint_path
    )
    app.run()