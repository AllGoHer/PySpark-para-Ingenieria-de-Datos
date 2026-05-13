# Estructura general de la API del lector de datos
spark.read.format("<source>").option("<key>", "<value>").schema(<schema>).load("<path>")


#Lectura de datos CSV mediante la API de Spark Data Reader
## Step 1: Crear un CSV File Usando PySpark
### Ejecutar el siguiente codigo en PySpark shell:
data = [
    (1, "Alice", "2024-01-01"),
    (2, "Bob", "2024-01-02"),
    (3, "Charlie", "2024-01-03")
]

columns = ["id", "name", "signup_date"]

df_source = spark.createDataFrame(data, columns)

df_source.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-data/users_csv")

### Step 2: Leer el archivo CSV mediante la API de Data Reader
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/opt/spark-data/users_csv")

### Step 3: Ejecución del disparador
df.show()
