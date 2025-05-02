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
        self.output_topic = output_topic
        self.checkpoint_dir = checkpoint_dir
        
        # Correct window durations as per requirements
        self.windows = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]

        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("btc-price-transformer") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        
        # Ensure checkpoint directory exists
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        # Define schema
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True)
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
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")
        
        # Convert string timestamp to proper timestamp
        # If your timestamp is in a specific format, add the format string as second parameter
        return parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))
    
    def compute_stats_for_window(self, df: DataFrame, window_duration: str) -> DataFrame:
        """
        Compute statistics for the given window duration.
        
        Returns a DataFrame with columns: window, symbol, avg_price, std_price
        """
        return df \
            .withWatermark("timestamp", "10 seconds")\
            .groupBy(
                window(col("timestamp"), window_duration),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol"),
                col("avg_price"),
                col("std_price"),
                # Add window duration as a string for the output format
                lit(window_duration).alias("window")
            )

    def process_all_windows(self, df: DataFrame) -> DataFrame:
        """
        Process all window durations and format output according to requirements.
        """
        # For each window, compute stats and add window name
        result_dfs = []
        
        for window_duration in self.windows:
            window_df = self.compute_stats_for_window(df, window_duration)
            result_dfs.append(window_df)
        
        # Union all window DataFrames
        union_df = result_dfs[0]
        for next_df in result_dfs[1:]:
            union_df = union_df.unionByName(next_df)
            
        return union_df
    
    def write_to_kafka(self, df: DataFrame) -> DataFrame:
        """
        Write the processed DataFrame to Kafka.
        """

        formatted_df = df.select(
            col("window_start").alias("timestamp"),
            col("symbol"),
            struct(
                col("window").alias("window"),
                col("avg_price").alias("avg_price"),
                col("std_price").alias("std_price")
            ).alias("window_stats")
        )

        kafka_df = formatted_df.selectExpr(
            "CAST(symbol AS STRING) as key",
            "to_json(struct(*)) AS value"
        )

        
        kafka_query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", self.checkpoint_dir) \
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
        
        # Process all windows and format output
        result_df = self.process_all_windows(df)
        
        # For debugging
        print("Output schema:")
        result_df.printSchema()
        
        # Write to Kafka
        query_kafka= self.write_to_kafka(result_df)
        
        # Let the query run until terminated
        query_kafka.awaitTermination()


if __name__ == "__main__":
    app = BTC_Price_Moving(
        kafka_bootstrap_servers="localhost:9092",
        input_topic="btc-price",
        output_topic="btc-price-moving",
        checkpoint_dir="/mnt/e/BigData/Lab04/spark_checkpoints/btc-price-moving"
    )
    app.run()