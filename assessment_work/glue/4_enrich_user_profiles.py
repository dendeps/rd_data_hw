from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

BUCKET = "den-data-platform-data-lake-923839912858"

customers_path = f"s3://{BUCKET}/silver/customers/"
profiles_path  = f"s3://{BUCKET}/silver/user_profiles/"
gold_path      = f"s3://{BUCKET}/gold/user_profiles_enriched/"

join_key = "email"  # <-- join by email

customers = spark.read.parquet(customers_path)
profiles  = spark.read.parquet(profiles_path)

# --- normalize join key to avoid case/space mismatch
def norm_email(col):
    return F.lower(F.trim(col))

customers = customers.withColumn("_email_norm", norm_email(F.col(join_key)))
profiles  = profiles.withColumn("_email_norm", norm_email(F.col(join_key)))

# drop null/empty emails (optional but recommended)
customers = customers.filter(F.col("_email_norm").isNotNull() & (F.col("_email_norm") != ""))
profiles  = profiles.filter(F.col("_email_norm").isNotNull() & (F.col("_email_norm") != ""))

# if profiles has duplicates per email - keep one (if you have ingest date, you can sort; else keep first)
profiles = profiles.dropDuplicates(["_email_norm"])

c = customers.alias("c")
p = profiles.alias("p")

joined = c.join(p, on=F.col("c._email_norm") == F.col("p._email_norm"), how="left")

# --- helper: overwrite customers fields with profiles values when present
def from_profile_or_customer(col_name: str):
    if col_name in profiles.columns:
        return F.coalesce(F.col(f"p.{col_name}"), F.col(f"c.{col_name}")).alias(col_name)
    return F.col(f"c.{col_name}").alias(col_name)

# 1) keep all customers columns, but fill first/last/state from profiles when possible
base_cols = []
for col_name in customers.columns:
    if col_name in ["first_name", "last_name", "state"]:
        base_cols.append(from_profile_or_customer(col_name))
    elif col_name == "_email_norm":
        # do not keep technical column in final output
        continue
    else:
        base_cols.append(F.col(f"c.{col_name}").alias(col_name))

# 2) add all profile columns that are not in customers (exclude join key duplicates + _email_norm)
extra_profile_cols = [
    F.col(f"p.{col_name}").alias(col_name)
    for col_name in profiles.columns
    if col_name not in customers.columns and col_name not in ["_email_norm"]
]

result = joined.select(*base_cols, *extra_profile_cols)

# --- write gold dataset
result.write.mode("overwrite").parquet(gold_path)

print("gold.user_profiles_enriched rows:", result.count())
result.printSchema()
