import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("[CONSUMER] Spark Delta Lake Modunda Başlatılıyor...")
time.sleep(30)

spark = SparkSession.builder \
    .appName("CryptoMarketLakehouse") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.500,"
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", StringType())
])

print("Kafka'ya bağlanılıyor...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "market_data") \
    .option("startingOffsets", "latest") \
    .load()

value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

print("Delta Lake'e yazılıyor...")

query = value_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "s3a://market-data/raw_layer_delta") \
    .option("checkpointLocation", "s3a://market-data/checkpoints_delta") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()