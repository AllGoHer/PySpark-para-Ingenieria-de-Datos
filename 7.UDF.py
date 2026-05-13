# Comparación del coste de las funciones definidas por el usuario (UDF) frente al de las funciones integradas
#UDF cost VS Buil-in Function

## Paso 1: Crear Dataset de ejemplo
data = [(i,) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["value"])


## Paso 2: Built-in Function
from pyspark.sql.functions import col

df_builtin = df.withColumn("double_value", col("value") * 2)

df_builtin.show()


## Paso 3: Python UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def double_value(x):
    return x * 2

double_udf = udf(double_value, IntegerType())

df_udf = df.withColumn("double_value", double_udf(col("value")))
df_udf.show()


## Paso 4: Observar el comportamiento
### Comparar:
#### •    Tiempo de ejecución
#### •    Duración de la tarea en Spark UI
#### •    Uso de la CPU

### La lógica es idéntica.
### El coste no lo es.