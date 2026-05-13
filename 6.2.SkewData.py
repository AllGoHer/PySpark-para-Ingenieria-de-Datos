# Paso 1: Crear Skewed Dataset
data = []
for i in range(100000):
    data.append(("HOT_KEY", i))

for i in range(1000):
    data.append((f"KEY_{i}", i))

df = spark.createDataFrame(data, ["key", "value"])


# Paso 2: Trigger a Shuffle Operation
df_grouped = df.groupBy("key").count()
df_grouped.show()


# Paso 3: Revisar el plan de ejecucion
df_grouped.explain()


# ESTRATEGIA DE MITIGACION DE DATOS SESGADOS

## Estrategia 1: Salting the Key
### Dividir una clave de acceso rapido en varias claves artificiales

from pyspark.sql.functions import rand, concat, lit

df_salted = df.withColumn(
    "salted_key",
    concat(df.key, lit("_"), (rand() * 10).cast("int"))
)

df_salted.groupBy("salted_key").count().show()


## Estrategia 2: Broadcast Join (Cuando es aplicable)
### Si una de las tablas o clave es mas pequeña:

from pyspark.sql.functions import broadcast

df_large.join(broadcast(df_small), "key")

## De esta forma se evita por completo el barajado (Shuffle).


# Estrategia 3: Filtrado previo a la agregación
## Reduzca los datos desde el principio.
df_filtered = df.filter(df.key != "HOT_KEY")

# La corrección del sesgo comienza con la reducción de datos.

