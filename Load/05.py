from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
import os

class Loader:
    def __init__(self, 
                 kafka_bootstrap_servers: str, 
                 input_topic: str, 
                 mongodb_uri: str, 
                 mongodb_database: str, 
                 checkpoint_dir: str):
        """
        Initialize the Loader class with Spark session and configurations.
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topic = input_topic
        self.mongodb_uri = mongodb_uri
        self.mongodb_database = mongodb_database
        self.checkpoint_dir = checkpoint_dir

        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("BTC-ZScore-Loader") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.mongodb.output.uri", self.mongodb_uri) \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Ensure checkpoint directory exists
        # os.makedirs(self.checkpoint_dir.replace("file://", ""), exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        

    def define_schema(self):
        """
        Define the schema for the btc-price-zscore Kafka topic.
        """
        zscore_schema = StructType([
            StructField("window", StringType(), True),
            StructField("zscore_price", DoubleType(), True)
        ])
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("zscores", ArrayType(zscore_schema), True)
        ])

    def load_stream(self):
        """
        Load streaming data from Kafka and write to MongoDB collections.
        """
        schema = self.define_schema()

        # Read from Kafka topic
        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON
        parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select(
                to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias("timestamp"),
                col("data.symbol"),
                col("data.zscores")
            )

        # Explode zscores array
        exploded_df = parsed_df.select(
            col("timestamp"),
            col("symbol"),
            explode(col("zscores")).alias("zscore")
        ).select(
            col("timestamp"),
            col("symbol"),
            col("zscore.window").alias("window"),
            col("zscore.zscore_price").alias("zscore_price")
        )

        # Apply watermark for late data handling
        watermarked_df = exploded_df.withWatermark("timestamp", "10 seconds")

        # Debug: Print schema and sample data to console
        print("Input schema:")
        watermarked_df.printSchema()

        console_query = watermarked_df.writeStream \
            .format("console") \
            .option("truncate", False) \
            .outputMode("append") \
            .start()

        # List of supported window intervals
        windows = ["30s", "1m", "5m", "15m", "30m", "1h"]

        queries = []
        for window_interval in windows:
            filtered_df = watermarked_df.filter(col("window") == window_interval)

            # Write to MongoDB collection
            query = filtered_df.writeStream \
                .format("mongodb") \
                .option("uri", self.mongodb_uri) \
                .option("database", self.mongodb_database) \
                .option("collection", f"btc-price-zscore-{window_interval}") \
                .option("checkpointLocation", os.path.join(self.checkpoint_dir, f"btc-zscore-{window_interval}")) \
                .outputMode("append") \
                .start()

            queries.append(query)
            print(f"Started streaming to collection: btc-price-zscore-{window_interval}")

        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    checkpoint_path = "file://" + os.path.abspath("../spark_checkpoints/loader")
    loader = Loader(
        kafka_bootstrap_servers="localhost:9092",
        input_topic="btc-price-zscore",
        mongodb_uri="mongodb://localhost:27017",
        mongodb_database="Lab04",
        checkpoint_dir=checkpoint_path
    )
    loader.load_stream()