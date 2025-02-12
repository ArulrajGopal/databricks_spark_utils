from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Sample Data with Multi-Level Nested Structs and Arrays
data = [
    (1, ("Alice", "Engineer", ("New York", "NY")), ["Python", "Spark"]),
    (2, ("Bob", "Manager", ("San Francisco", "CA")), ["Java", "Scala"])
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("job", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("state", StringType(), True)
        ]))
    ])),
    StructField("skills", ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)

# Apply Multi-Level Flattening and Exploding
df_flat = flatten_and_explode(df)
df_flat.show(truncate=False)
