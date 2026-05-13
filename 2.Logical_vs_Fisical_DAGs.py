#Step 1: Create a Dataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [(i % 5, i) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["category", "value"])


#Step 2: Define Transformations (No Execution)
result_df = (
    df.filter(col("value") > 1000)
      .groupBy("category")
      .count()
)


#Step 3: Inspect the Physical Plan
result_df.explain(mode="formatted")

#Step 4: Trigger Execution and Verify
result_df.collect()


