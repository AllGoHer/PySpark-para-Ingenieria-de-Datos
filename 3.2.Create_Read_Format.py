#Crear datos de origen
## Step 1: Crear el siguiente codigo en PySpark shell:
data = [(i, f"name_{i}", i * 100, "2024-01-01") for i in range(1_000_000)]

columns = ["id", "name", "salary", "date"]

df = spark.createDataFrame(data, columns)


##Step 2: Write Data in All Three Formats

df.write.mode("overwrite").option("header", "true").csv("/opt/spark-data/employees_csv")

df.write.mode("overwrite").json("/opt/spark-data/employees_json")

df.write.mode("overwrite").parquet("/opt/spark-data/employees_parquet")

#Step 3: Leer CSV
csv_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark-data/employees_csv")

csv_df.select("name").count()

# Step 4: Leer JSON
json_df = spark.read \
    .option("inferSchema", "true") \
    .json("/opt/spark-data/employees_json")

json_df.select("name").count()

# Step 5: Leer Parquet
parquet_df = spark.read.parquet("/opt/spark-data/employees_parquet")

parquet_df.select("name").count()




