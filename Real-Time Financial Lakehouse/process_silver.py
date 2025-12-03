import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, stddev_pop, avg, current_timestamp, lit
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

print("Silver (Veri Ön Isleme Ve Model Eğitimi) Delta Lake Modu Başlatılıyor...")
time.sleep(10)

spark = SparkSession.builder \
    .appName("CryptoSilverLayer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.500,io.delta:delta-core_2.12:2.4.0") \
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

MODEL_PATH = "s3a://market-data/models/btc_price_model"
ml_model = None


def load_model():
    global ml_model
    try:
        if ml_model is None:
            ml_model = LinearRegressionModel.load(MODEL_PATH)
            print("\n Model Yüklendi Tahminler Başlıyor.\n")
        return True
    except:
        return False

load_model()

raw_df = spark.readStream \
    .format("delta") \
    .load("s3a://market-data/raw_layer_delta")

clean_df = raw_df.select(col("symbol"), col("price").cast("double"), col("timestamp").cast("timestamp"))

# Analiz
windowed_df = clean_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("symbol")) \
    .agg(
    stddev_pop("price").alias("volatility"),
    avg("price").alias("average_price"),
    current_timestamp().alias("processed_time")
)


def process_batch(batch_df, batch_id):
    global ml_model

    if batch_df.count() > 0:
        if ml_model is None:
            load_model()

        batch_df = batch_df.na.fill(0, subset=["volatility"])
        final_df = batch_df

        if ml_model is not None:
            try:
                assembler = VectorAssembler(inputCols=["volatility"], outputCol="features")
                vec_df = assembler.transform(batch_df)
                predictions = ml_model.transform(vec_df)
                final_df = predictions.select("symbol", "volatility", "average_price", "processed_time",
                                              col("prediction").alias("predicted_price"))
            except:
                final_df = batch_df.withColumn("predicted_price", lit(0.0))
        else:
            final_df = batch_df.withColumn("predicted_price", lit(0.0))

        final_df.write \
            .format("delta") \
            .mode("append") \
            .save("s3a://market-data/silver_layer_delta")


query = windowed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "s3a://market-data/checkpoints_silver_delta") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()