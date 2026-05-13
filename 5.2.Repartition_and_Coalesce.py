# Paso 1: Crear Dataset
data = (
    [(0, i) for i in range(90_000)] +
    [(1, i) for i in range(25_000)] +
    [(2, i) for i in range(25_000)] +
    [(3, i) for i in range(25_000)] +
    [(4, i) for i in range(25_000)]
)
df = spark.createDataFrame(data, ["key", "value"])

## Paso 2: Check Default Partitions
df.rdd.getNumPartitions()


## Paso 3: Repartition Data
df_repart = df.repartition()
df_repart.rdd.getNumPartitions()
df_repart.count()

### Revisar la interfaz de usuario de Spark:
#### •    Barajado completo
#### •    Mayor paralelismo de tareas

## Paso 4: Coalesce Data
df_repart = df.repartition(5, "key")
df_coalesced = df_repart.coalesce(10)
df_coalesced.rdd.getNumPartitions()
df_coalesced.count()

### Revisar la interfaz de usuario de Spark:
#### •    No hay barajado
#### •    Menos tareas
#### •    Duración desigual de las tareas