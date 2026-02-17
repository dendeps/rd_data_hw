import sys
import boto3
import csv
import io
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# -----------------------------
# Ініціалізація Glue/Spark
# -----------------------------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

BUCKET_NAME = "den-data-platform-data-lake-923839912858"
RAW_PREFIX = "raw/customers/"
BRONZE_PREFIX = "bronze/customers/2022-08-5/"

s3 = boto3.client('s3')

# -----------------------------
# 0️⃣ Знаходимо перший CSV у S3 для схемы
# -----------------------------
csv_keys = []

paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
    for obj in page.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv'):
            csv_keys.append(key)

if not csv_keys:
    raise ValueError(f"No CSV files found in s3://{BUCKET_NAME}/{RAW_PREFIX}")

# Беремо перший файл для читання header
first_csv = csv_keys[0]
obj = s3.get_object(Bucket=BUCKET_NAME, Key=first_csv)
header = csv.reader(io.TextIOWrapper(obj['Body'], encoding='utf-8'))
columns = next(header)

# -----------------------------
# 1️⃣ Створюємо схему з усіх колонок як StringType
# -----------------------------
schema = StructType([StructField(col, StringType(), True) for col in columns])

# -----------------------------
# 2️⃣ Завантаження всіх CSV як String
# -----------------------------
raw_path = f"s3://{BUCKET_NAME}/{RAW_PREFIX}"
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("recursiveFileLookup", "true")\
    .schema(schema)\
    .load(raw_path)

# -----------------------------
# 3️⃣ Збереження у Bronze (S3)
# -----------------------------
bronze_path = f"s3://{BUCKET_NAME}/{BRONZE_PREFIX}"
df.write.mode("overwrite").parquet(bronze_path)

print(f"Bronze data written: {df.count()} rows")
df.printSchema()
