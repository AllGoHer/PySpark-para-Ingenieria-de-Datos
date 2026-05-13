# Ranking with Window Functions

## Paso 1: Crear Dataset de ejemplo
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 300)
]

df = spark.createDataFrame(data, ["user", "date", "amount"])


# Paso 2: Definir Window
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("user").orderBy("amount")


# Paso 3: aplicar Window Function
df_ranked = df.withColumn("rank", row_number().over(window_spec))
df_ranked.show()


# Paso 4: Revisar el plan de ejecucion
df_ranked.explain()

