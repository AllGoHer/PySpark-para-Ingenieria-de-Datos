# Paso 1: Crear Data de ejemplo
data = [(i, f"user_{i}") for i in range(100)]

df = spark.createDataFrame(data, ["id", "name"])


# Paso 2: guardar en formato Parquet
output_path = "/opt/spark-data/spark_writer_demo"

df.write.mode("overwrite").parquet(output_path)


# Paso 3: Revisar el resultado
## Observa:
### •    Varios archivos 
### •    No hay un único archivo de salida
### •    Salida organizada por directorios

# Paso 4: Controlar el número de archivos
df.coalesce(1).write.mode("overwrite").parquet("/opt/spark-data/spark_single_file")

# Ten en cuenta lo siguiente:
## •    Menos archivos
## •    Menor paralelismo
## •    Posible cuello de botella
