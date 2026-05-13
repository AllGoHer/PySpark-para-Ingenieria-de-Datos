# Paso 1:  Crear datos de ejemplo anidados    
## en PySpark shell:
data = [
    (1, "Alice", Row(city="NY", country="USA")),
    (2, "Bob", Row(city="LA", country="USA"))]


df = spark.createDataFrame(data, [“id”, “name”, “location”])

## Paso 2: Escribir datos en formato JSON
df.write \
    .mode("overwrite") \
    .json("/opt/spark-data/nested_users")

## Paso 3: Definir Esquema usando StructType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

struct_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField(
        "location",
        StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]),
        True
    )
])

## Paso 4: Leer usando Esquema StructType df_struct = spark.read \
    .schema(struct_schema) \
    .json("/opt/spark-data/nested_users")

df_struct.printSchema()
df_struct.show()


## Paso 5: Definir esquema DDL equivalente
ddl_schema = """
id INT,
name STRING,
location STRUCT<city:STRING, country:STRING>
"""

## Paso 6: Leer usando Esquema DDL 

df_ddl = spark.read \
    .schema(ddl_schema) \
    .json("/opt/spark-data/nested_users")

df_ddl.printSchema()
df_ddl.show()

