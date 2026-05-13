# CREANDO Y BORRANDO TABLAS
##  Objectivo
### Observar con claridad el comportamiento del ciclo de vida.

## Paso 1: Crear tabla gestionada
data = [(1, "A"), (2, "B")]
df = spark.createDataFrame(data, ["id", "value"])

df.write.mode("overwrite").saveAsTable("managed_demo")


# Paso 2: Crear tabla externa
df.write \
  .mode("overwrite") \
  .option("path", "/opt/spark-data/external_demo") \
  .saveAsTable("external_demo")

spark.sql("SELECT * FROM external_demo").show()
spark.sql("SELECT * FROM managed_demo ").show()

# Paso 3: Borrar Tablas
spark.sql("DROP TABLE managed_demo")
spark.sql(“DROP TABLE external_demo”)

# Obsérvese:
## • Datos gestionados eliminados
## • Los datos externos siguen existiendo
