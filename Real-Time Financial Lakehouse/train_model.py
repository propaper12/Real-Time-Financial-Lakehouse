import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col

print(" Eğitim Başlıyor...")

spark = SparkSession.builder \
    .appName("TrainCryptoModel") \
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

try:
    print("Veri Okunuyor: silver_layer_delta")
    df = spark.read.format("delta").load("s3a://market-data/silver_layer_delta")

    clean_data = df.filter((col("average_price") > 0) & (col("volatility") >= 0))
    count = clean_data.count()
    print(f"Veri Sayısı: {count}")

    if count < 5:
        print("Yetersiz Veri.")
        sys.exit(1)

except Exception as e:
    print(f"HATA: {e}")
    sys.exit(1)

assembler = VectorAssembler(inputCols=["volatility"], outputCol="features")
train_data = assembler.transform(clean_data).select("features", "average_price")

lr = LinearRegression(featuresCol="features", labelCol="average_price")
model = lr.fit(train_data)

print(f"Model Eğitildi Katsayı: {model.coefficients}")

model.write().overwrite().save("s3a://market-data/models/btc_price_model")
print("Model Kaydedildi.")
spark.stop()