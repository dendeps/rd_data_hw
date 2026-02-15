from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

BUCKET_NAME = "den-data-platform-data-lake-923839912858"

bronze_path = f"s3://{BUCKET_NAME}/bronze/sales/"
silver_path = f"s3://{BUCKET_NAME}/silver/sales/"

df = spark.read.parquet(bronze_path)

# --- normalize purchasedate: yyyy-M-d -> yyyy-MM-dd
parts = F.split(F.col("purchasedate"), "-")
df = df.withColumn(
    "purchase_date_str",
    F.concat_ws(
        "-",
        parts.getItem(0),
        F.lpad(parts.getItem(1), 2, "0"),
        F.lpad(parts.getItem(2), 2, "0"),
    )
)

# --- make proper DATE column
df = df.withColumn("purchase_date", F.to_date("purchase_date_str", "yyyy-MM-dd"))

# --- rename колонок під silver
# (підстав свої реальні назви колонок з bronze, якщо вони трохи інші)
df = (df
      .withColumnRenamed("customerid", "client_id")
      .withColumnRenamed("product", "product_name")
      .withColumnRenamed("price", "price")
)

# --- basic cleanup
df = (df
      .withColumn("client_id", F.trim(F.col("client_id")))
      .withColumn("product_name", F.trim(F.col("product_name")))
      .withColumn("price", F.regexp_replace(F.col("price"), ",", ".").cast(DoubleType()))
)

# drop bad rows (як мінімум без дати)
df = df.na.drop(subset=["purchase_date", "client_id", "product_name"])

# --- write silver partitioned
(df.write
   .mode("overwrite")
   .partitionBy("purchase_date_str")   # стабільні партиції: purchase_date_str=2022-09-01/
   .parquet(silver_path))

print("Silver rows:", df.count())
df.printSchema()
