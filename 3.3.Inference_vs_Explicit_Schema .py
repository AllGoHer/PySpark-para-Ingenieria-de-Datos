# Paso 1: Crear un archivo CSV con tipos mixtos
## Ejecutar el siguiente codigo en PySpark shell:
data = [
    ("1", "Alice", "1000"),
    ("2", "Bob", "1500.50"),
    ("3", "Charlie", "invalid")
]

columns = ["id", "name", "salary"]

df_raw = spark.createDataFrame(data, columns)

df_raw.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-data/schema_test")


## Paso 2: Leer usando Esquema Inferencial

df_infer = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark-data/schema_test")

df_infer.printSchema()
df_infer.show()

## Paso 3: Definir Esquema Explicito usando StructType
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])

## Paso 4: Leer usando Esquema Explicito 
df_explicit = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/opt/spark-data/schema_test")

df_explicit.printSchema()
df_explicit.show()



