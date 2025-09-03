import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, window, avg, min, max, sum, to_timestamp
from cassandra.cluster import Cluster

# ---------------- Kafka config ---------------- #
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "stock-data"

kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "SCRAM-SHA-512"

# ---------------- Cassandra setup ---------------- #
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS stockks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS stockks.aggregated_stock_data (
        ticker TEXT,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        avg_open DOUBLE,
        avg_close DOUBLE,
        min_low DOUBLE,
        max_high DOUBLE,
        total_volume BIGINT,
        PRIMARY KEY ((ticker), window_end)
    );
    """)
    print("Table aggregated_stock_data created successfully!")

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

# ---------------- Spark setup ---------------- #
spark = SparkSession.builder \
    .master("local") \
    .appName("StockDataSASL") \
    .config("spark.kafka.sasl.jaas.config", "file:./custom_jaas.conf") \
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logging.info("Spark connection created successfully!")

# ---------------- Kafka read ---------------- #
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# ---------------- Transform stock data ---------------- #
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

stock_df = df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), json_schema).alias("data")) \
             .select("data.*") \
             .withColumn("timestamp", to_timestamp(col("timestamp")))

# ---------------- Windowed Aggregation ---------------- #
windowed_df = stock_df.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("ticker")
).agg(
    avg("open").alias("avg_open"),
    avg("close").alias("avg_close"),
    min("low").alias("min_low"),
    max("high").alias("max_high"),
    sum("volume").alias("total_volume")
).select(
    col("ticker"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_open"),
    col("avg_close"),
    col("min_low"),
    col("max_high"),
    col("total_volume")
)

# ---------------- Write to Cassandra ---------------- #
def writeToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="aggregated_stock_data", keyspace="stockks") \
        .mode("append") \
        .save()
    print(f"Batch {epochId} written to Cassandra successfully!")

cassandra_session = create_cassandra_connection()
if cassandra_session:
    create_keyspace(cassandra_session)
    create_table(cassandra_session)

# ---------------- Start Streaming ---------------- #
query = windowed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(writeToCassandra) \
    .option("checkpointLocation", "./tmp/checkpoint_stock_sasl") \
    .start()

query.awaitTermination()
