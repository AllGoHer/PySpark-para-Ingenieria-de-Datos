# Comparación SQL y DataFrame

# Paso 1: Crear Tabla
data = [(1, 500), (2, 1500), (3, 2500)]
df = spark.createDataFrame(data, ["id", "amount"])

df.createOrReplaceTempView("sales")


# Paso 2: DataFrame Query
df_df = df.filter(df.amount > 1000)
df_df.explain(True)


# Paso 3: SQL Query
df_sql = spark.sql("SELECT * FROM sales WHERE amount > 1000")
df_sql.explain(True)

# Compara:
# • Planos físicosDeben coincidir.
