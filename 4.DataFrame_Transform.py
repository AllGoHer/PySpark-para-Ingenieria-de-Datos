# Comparación entre transformaciones de rango reducido y de rango amplio en Spark UI

# Paso 1: Crear datos de ejemplo
## en PySpark shell:
data = [(i, i % 5) for i in range(1_000_000)]

df = spark.createDataFrame(data, ["id", "group"])

## Paso 2: Aplicar una transformación de estrechamiento (NARROW)
df_narrow = df.filter(df.group > 2).select("id")

df_narrow.show()

## Paso 3: Revisar la interfaz de usuario de Spark (caso concreto)
### Abre la interfaz de usuario de Spark → pestaña «Jobs» y comprueba lo siguiente:
#### •    Un solo trabajo
#### •    Una sola etapa
#### •    No hay métricas de lectura/escritura aleatoria
#### •    Las tareas se ejecutan de forma independiente dentro de las particiones

## Paso 4: Aplicar una transformación amplia (WIDE)
df_wide = df.groupBy("group").count()

df_wide.show()

## Paso 5: Revisar la interfaz de usuario de Spark (caso de transformación amplia)
### Abre la interfaz de usuario de Spark → pestaña «Stages» y observa lo siguiente:
#### •    Se han creado varias etapas
#### •    Las métricas de lectura aleatoria y escritura aleatoria son visibles
#### •    Una etapa escribe datos aleatorios
#### •    La siguiente etapa lee los datos aleatorios

### Ahora abre la interfaz de usuario de Spark → pestaña «SQL / DAG»:
#### •    Identifica el límite de la aleatorización
#### •    Fíjate en que la etapa se divide en «groupBy»

### Esto confirma que se trata de una transformación amplia.


