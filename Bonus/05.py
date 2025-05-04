from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json, expr, lit, min as min_, coalesce, to_json, struct, to_timestamp

class Bonus:
    def __init__(self,
                 kafka_bootstrap_servers: str,
                 higher_topic: str,
                 lower_topic: str,
                 checkpoint_higher_dir: str,
                 checkpoint_lower_dir: str) -> None:
        """
        Initialize the Bonus class with Kafka parameters and Spark session.
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.higher_topic = higher_topic
        self.lower_topic = lower_topic
        self.checkpoint_higher_dir = checkpoint_higher_dir
        self.checkpoint_lower_dir = checkpoint_lower_dir
        
        # Initialize Spark session with necessary configurations
        self.spark = SparkSession.builder\
                    .appName("btc-bonus")\
                    .config("spark.sql.shuffle.partitions", "1")\
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
                    .getOrCreate()
        
        # Schema for the incoming JSON data from Kafka
        self.schema = StructType()\
                    .add("timestamp", StringType())\
                    .add("price", DoubleType())
        
    def read_streaming_data(self) -> DataFrame:
        """
        Read streaming data from the Kafka topic 'btc-price'.
        Apply watermark to handle late data with 10-second tolerance.
        """
        # Read raw data from Kafka
        btc_raw_df = self.spark.readStream\
                    .format("kafka")\
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)\
                    .option("subscribe", "btc-price")\
                    .option("startingOffsets", "earliest")\
                    .load()
        
        # Parse JSON data and convert timestamp to proper format
        # Apply watermark for handling late data (10 seconds tolerance)
        btc_parsed_df = btc_raw_df.selectExpr("CAST(value AS STRING)")\
                        .select(from_json(col("value"), self.schema).alias("data"))\
                        .selectExpr("to_timestamp(data.timestamp) as event_time", "data.price as price") \
                        .withWatermark("event_time", "10 seconds")
        
        return btc_parsed_df
    
    def find_higher_and_lower(self, parsed_df: DataFrame) -> tuple:
        """
        For each price record, find the first higher and lower prices within a 20-second window.
        Uses DataFrame operations to properly handle streaming joins with watermarks.
        """
        # Add a join key to both DataFrames for equality condition
        df1 = parsed_df.withColumn("join_key", lit(1)).alias("df1")
        df2 = parsed_df.withColumn("join_key", lit(1)).alias("df2")

        # Define the join condition with equality predicate
        join_condition = (col("df1.join_key") == col("df2.join_key")) & \
                        (col("df2.event_time") > col("df1.event_time")) & \
                        (col("df2.event_time") <= expr("df1.event_time + interval 20 seconds"))
        
        # Join with the proper watermark condition to make stream-stream join work
        joined_df = df1.join(
            df2,
            join_condition,
            "leftOuter"
        )
        
        # Calculate time difference in seconds between events
        joined_df = joined_df.withColumn(
            "time_diff",
            (col("df2.event_time").cast("long") - col("df1.event_time").cast("long")).cast("double")
        )
        
        # Filter for higher price records and calculate min time difference
        higher_df = joined_df.filter(col("df2.price") > col("df1.price")) \
            .groupBy("df1.event_time") \
            .agg(min_("time_diff").alias("time_diff")) \
            .select(
                col("event_time").alias("timestamp"),
                col("time_diff").alias("higher_window")
            )
        
        # Filter for lower price records and calculate min time difference
        lower_df = joined_df.filter(col("df2.price") < col("df1.price")) \
            .groupBy("df1.event_time") \
            .agg(min_("time_diff").alias("time_diff")) \
            .select(
                col("event_time").alias("timestamp"),
                col("time_diff").alias("lower_window")
            )
        
        # Find all distinct timestamps for base records
        base_df = parsed_df.select(col("event_time").alias("timestamp")).distinct()
        
        # Join with the base timestamps and fill missing values with 20.0 seconds
        final_higher_df = base_df.join(higher_df, "timestamp", "left_outer") \
            .withColumn("higher_window", coalesce(col("higher_window"), lit(20.0)))
        
        final_lower_df = base_df.join(lower_df, "timestamp", "left_outer") \
            .withColumn("lower_window", coalesce(col("lower_window"), lit(20.0)))
        
        # Format output for Kafka in JSON format as per requirements
        final_higher_df = higher_df.select(
            to_json(struct(col("timestamp").cast("string").alias("timestamp"), "higher_window")).alias("value")
        )
        
        final_lower_df = lower_df.select(
            to_json(struct(col("timestamp").cast("string").alias("timestamp"), "lower_window")).alias("value")
        )
        
        return final_higher_df, final_lower_df

    def write_to_kafka(self, final_higher_df: DataFrame, final_lower_df: DataFrame) -> tuple:
        """
        Write the processed DataFrames to Kafka topics.
        """
        # Write higher price windows to Kafka topic
        higher_query = final_higher_df.writeStream\
                    .format("kafka")\
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)\
                    .option("topic", self.higher_topic)\
                    .option("checkpointLocation", self.checkpoint_higher_dir)\
                    .outputMode("append")\
                    .start()
        
        # Write lower price windows to Kafka topic
        lower_query = final_lower_df.writeStream\
                    .format("kafka")\
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)\
                    .option("topic", self.lower_topic)\
                    .option("checkpointLocation", self.checkpoint_lower_dir)\
                    .outputMode("append")\
                    .start()
        
        return higher_query, lower_query
        
    def run(self) -> None:
        """
        Run the entire BTC price window finder process.
        """
        # Step 1: Read and parse data from Kafka with watermark for late data
        parsed_df = self.read_streaming_data()
        
        # Step 2: Find first higher and lower prices within 20-second window
        higher_df, lower_df = self.find_higher_and_lower(parsed_df)
        
        # Step 3: Write results to Kafka topics
        higher_query, lower_query = self.write_to_kafka(higher_df, lower_df)

        # Wait for streaming to terminate
        higher_query.awaitTermination()
        lower_query.awaitTermination()


if __name__ == "__main__":
    # Initialize and run the application
    btc_bonus = Bonus(
        kafka_bootstrap_servers="localhost:9092",
        higher_topic="btc-price-higher",
        lower_topic="btc-price-lower",
        checkpoint_higher_dir="/tmp/high-checkpoint",
        checkpoint_lower_dir="/tmp/low-checkpoint"
    )
    btc_bonus.run()
