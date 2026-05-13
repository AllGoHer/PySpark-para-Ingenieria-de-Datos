# Shuffle vs Broadcast Join


## Paso 1: Crear un Dataset largo
### en PySpark shell:
orders = [(i, i % 1000, i * 10) for i in range(1_000_000)]

df_orders = spark.createDataFrame(
    orders,
    ["order_id", "customer_id", "amount"]
)

## Paso 2: Crear un pequeño conjunto de datos de referencia
customers = [(i, f"Customer_{i}") for i in range(1000)]

df_customers = spark.createDataFrame(
    customers,
    ["customer_id", "name"]
)

## Paso 3: Shuffle Join (Default)
df_join_shuffle = df_orders.join(df_customers, "customer_id")

df_join_shuffle.show()

## Revisa la interfaz de usuario de Spark
### Abre la interfaz de usuario de Spark → pestaña «Stages / SQL» y observa:
#### •    Dos etapas de reordenación
#### •    Métricas elevadas de lectura y escritura en la reordenación
#### •    Tráfico de red y E/S de disco implicados

### Esta es la unión basada en reordenación predeterminada de Spark.

## Paso 4: Broadcast Join (Explicit)
from pyspark.sql.functions import broadcast

df_join_broadcast = df_orders.join(
    broadcast(df_customers),
    "customer_id"
)

df_join_broadcast.show()

## Examine la interfaz de usuario de Spark
### Abra la interfaz de usuario de Spark → pestaña SQL / DAG y observe lo siguiente:
#### •    No hay reorganización para el conjunto de datos grande
#### •    Intercambio de difusión en lugar de intercambio de reorganización
#### •    Ejecución más rápida
#### •    Unión en una sola etapa para el lado grande