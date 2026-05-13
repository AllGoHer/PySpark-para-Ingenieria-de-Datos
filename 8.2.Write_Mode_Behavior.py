# Observación del comportamiento del modo de escritura

# Paso 1: Crear Dataset
data = [(1, "A"), (2, "B")]
df = spark.createDataFrame(data, ["id", "value"])


# Paso 2: Escritura inicial
path = "/opt/spark-data/write_mode_demo"
df.write.mode("overwrite").parquet(path)


# Paso 3: Añadir los mismos datos
df.write.mode("append").parquet(path)

## Fíjate en lo siguiente:
### • Registros duplicados

# Paso 4: Sobrescribir de nuevo
df.write.mode("overwrite").parquet(path)

## Observa:
### • Datos antiguos sustituidos
### • Comportamiento atómico
