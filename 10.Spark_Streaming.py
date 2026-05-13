# Son estructuralmente equivalentes
spark.read.parquet("/data/events")

spark.readStream.parquet("/data/events")

# Ambos:
## • Crean un plan lógico
## • Procesarlo en Catalyst
## • Generan un plan físico
## • Ejecutan en Spark Executors