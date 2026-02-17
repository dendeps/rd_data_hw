from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

BUCKET_NAME = "den-data-platform-data-lake-923839912858"
bronze_path = f"s3://{BUCKET_NAME}/bronze/customers/"
silver_path = f"s3://{BUCKET_NAME}/silver/customers/"

df = spark.read.parquet(bronze_path)

# normalize column names to lowercase (важливо!)
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower())


# --- 1) базова чистка (всі як строки в bronze)
for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c)))


# --- 2) перейменування колонок (bronze лишає оригінальні назви з CSV)
# ПІДСТАВ ТОЧНІ НАЗВИ З ТВОГО CSV (нижче типові приклади).
# Якщо твої назви інші — просто заміни ключі у withColumnRenamed.
rename_map = {
    "id": "client_id",
    "firstname": "first_name",
    "lastname": "last_name",
    "email": "email",
    "registrationdate": "registration_date",
    "state": "state",
}

for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# --- 3) нормалізація email
if "email" in df.columns:
    df = df.withColumn("email", F.lower(F.col("email")))

# --- 4) нормалізація registration_date (якщо там буває yyyy-M-d)
# робимо спочатку строку yyyy-MM-dd, потім to_date
if "registration_date" in df.columns:
    parts = F.split(F.col("registration_date"), "-")
    df = df.withColumn(
        "registration_date_str",
        F.when(
            F.size(parts) == 3,
            F.concat_ws(
                "-",
                parts.getItem(0),
                F.lpad(parts.getItem(1), 2, "0"),
                F.lpad(parts.getItem(2), 2, "0"),
            ),
        ).otherwise(F.col("registration_date"))
    )
    df = df.withColumn("registration_date", F.to_date("registration_date_str", "yyyy-MM-dd")) \
           .drop("registration_date_str")

# --- 5) ДЕДУП: залишити 1 рядок на client_id
# Найкраще — брати найновіший по даті файлу. Спробуємо витягнути дату з input_file_name().
# Якщо у шляху є .../2022-09-10/... або filename містить 2022-09-10 — спрацює.
df = df.withColumn("_src", F.input_file_name())
df = df.withColumn("_ingest_date_str", F.regexp_extract(F.col("_src"), r"(\d{4}-\d{2}-\d{1,2})", 1))

# нормалізуємо _ingest_date_str до yyyy-MM-dd
parts2 = F.split(F.col("_ingest_date_str"), "-")
df = df.withColumn(
    "_ingest_date_str",
    F.when(
        F.col("_ingest_date_str") != "",
        F.concat_ws(
            "-",
            parts2.getItem(0),
            F.lpad(parts2.getItem(1), 2, "0"),
            F.lpad(parts2.getItem(2), 2, "0"),
        ),
    ).otherwise(F.lit(None))
)
df = df.withColumn("_ingest_date", F.to_date(F.col("_ingest_date_str"), "yyyy-MM-dd"))

# якщо дату з файлу не витягнуло — просто dedup по client_id (останній “який попався”)
if "client_id" in df.columns:
    w = Window.partitionBy("client_id").orderBy(F.col("_ingest_date").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")
           
print("BRONZE COLUMNS:", df.columns)
df.printSchema()


# --- 6) залишаємо тільки потрібні silver колонки
wanted = ["client_id", "first_name", "last_name", "email", "registration_date", "state"]
df = df.select([c for c in wanted if c in df.columns])

# --- 7) запис в silver (без partitionBy)
df.write.mode("overwrite").parquet(silver_path)

print("Silver customers rows:", df.count())
df.printSchema()
