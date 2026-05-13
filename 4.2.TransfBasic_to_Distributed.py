# Step 1: Crear datos de ejemplo
## ejecutar en PySpark shell:
from pyspark.sql.functions import col

data = [(i, i % 10) for i in range(500_000)]

df = spark.createDataFrame(data, ["id", "group"])

# Paso 2: Enlazar transformaciones básicas
df_transformed = (
    df.select("id", "group")
      .filter(col("group") > 5)
      .withColumn("group_double", col("group") * 2)
      .withColumn("group_plus_one", col("group") + 1)
)

# Paso 3: Revisar el plan lógico
df_transformed.explain()

# Paso 4: Activar la ejecución
df_transformed.show()

# Paso 5: Revisar la interfaz de usuario de Spark
## Abre la interfaz de usuario de Spark → pestaña «Jobs / Stages» y comprueba lo siguiente:
### •    Se ha creado un trabajo
### •    Se ha ejecutado una etapa
### •    No se ha realizado ningún reordenamiento
### •    El tiempo de CPU de la tarea refleja:
#### o    Cálculos de varias columnas
#### o    El coste de la evaluación de expresiones


#Esto confirma la acumulación del coste de las transformaciones:
#todas las transformaciones se ejecutan juntas cuando se invoca una acción.