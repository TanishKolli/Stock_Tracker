import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp
from cassandra.cluster import Cluster

# ---------------- Cassandra setup ---------------- #
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS stockks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS stockks.stock_data (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        timestamp TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT
    );
    """)
    print("Table stock_data created successfully!")

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

# ---------------- Spark setup ---------------- #
def create_spark_connection():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("StockDataStreaming") \
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create Spark session due to exception: {e}")
    return spark

# ---------------- Kafka read ---------------- #
def connect_to_kafka(spark_conn):
    try:
        df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "stock-data") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka dataframe created successfully")
        return df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

# ---------------- Transform stock data ---------------- #
def create_selection_df_from_kafka(spark_df):
    json_schema = StructType([
        StructField("id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
                 .select(from_json(col("value"), json_schema).alias("data")) \
                 .select("data.*")

    # Convert timestamp string to Spark TimestampType
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    return df

# ---------------- Write to Cassandra per micro-batch ---------------- #
def write_to_cassandra_batch(df, epoch_id):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .options(table="stock_data", keyspace="stockks") \
      .mode("append") \
      .save()
    print(f"Batch {epoch_id} written to Cassandra successfully!")

# ---------------- Main ---------------- #
if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            stock_df = create_selection_df_from_kafka(kafka_df)
            cassandra_session = create_cassandra_connection()
            if cassandra_session:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                print("Streaming is being started...")

                streaming_query = stock_df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(write_to_cassandra_batch) \
                    .option("checkpointLocation", "./tmp/checkpoint_stock") \
                    .start()

                streaming_query.awaitTermination()
