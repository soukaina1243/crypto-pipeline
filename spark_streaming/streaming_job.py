
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "crypto-prices"

#JDBC_URL = "jdbc:postgresql://localhost:5433/crypto_pipeline"  # swap to cloud host on Day 3
JDBC_URL = "jdbc:postgresql://aws-0-eu-central-1.pooler.supabase.com:5432/postgres"
JDBC_PROPERTIES = {
    "user": "postgres.fjgwwsffebkdjrghtqkv",
    "password": "ur password",
    "driver": "org.postgresql.Driver",
}
schema = StructType([
    StructField("coin", StringType()),
    StructField("price_usd", DoubleType()),
    StructField("timestamp", LongType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crypto-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

def read_kafka_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    # Kafka gives us a unix timestamp (seconds) — Spark needs a proper timestamp column
    parsed = parsed.withColumn("event_time", (col("timestamp")).cast("timestamp"))
    return parsed


def windowed_aggregation(df):
    windowed = (
        df.withWatermark("event_time", "1 minutes")
          .groupBy(window(col("event_time"), "1 minutes"), col("coin"))
          .agg(
              avg("price_usd").alias("avg_price"),
              spark_min("price_usd").alias("min_price"),
              spark_max("price_usd").alias("max_price"),
          )
          .select(
              col("window.start").alias("window_start"),
              col("window.end").alias("window_end"),
              col("coin"),
              col("avg_price"),
              col("min_price"),
              col("max_price"),
          )
    )
    return windowed
    
    


def write_to_postgres(batch_df, batch_id):
    
    try:
        batch_df.write.jdbc(
            url=JDBC_URL,
            table="crypto_windowed_prices",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
        print(f"[OK] batch {batch_id} written, {batch_df.count()} rows")
    except Exception as e:
        print(f"[WARN] batch {batch_id} failed: {e}")
   


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    stream = read_kafka_stream(spark)
    windowed = windowed_aggregation(stream)

    query = (
        windowed.writeStream
        .foreachBatch(write_to_postgres)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "checkpoints/crypto_windowed")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
