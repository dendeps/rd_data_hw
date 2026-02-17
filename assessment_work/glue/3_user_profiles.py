from pyspark.context import SparkContext
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

BUCKET = "den-data-platform-data-lake-923839912858"

raw_path = f"s3://{BUCKET}/raw/user_profiles/"          # JSONLines тут
silver_path = f"s3://{BUCKET}/silver/user_profiles/"   # parquet сюди

# JSONLines = звичайний json reader (Spark сам читає line-delimited)
df = spark.read.json(raw_path)

# (якість ідеальна) - просто пишемо
df.write.mode("overwrite").parquet(silver_path)

print("user_profiles silver rows:", df.count())
df.printSchema()
