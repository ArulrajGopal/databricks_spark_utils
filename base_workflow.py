# Databricks notebook source
# 1. read a file into dataframe
# 2. rename few columns (using selectExpr)
# 3. join with parent table, which should have atlease two joining key and get 1 or two key
# 4. rename those columns using withColumn
# 5. do many aggregations
# 6. do windows functions
# 7. write it some where

# COMMAND ----------

# MAGIC %sql
# MAGIC USE HIVE_METASTORE;
# MAGIC CREATE DATABASE IF NOT EXISTS DEMO;
# MAGIC USE DEMO;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType, LongType
from pyspark.sql.functions import min, max, count

# COMMAND ----------

spark.conf.set("fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net","<account-key>")


# COMMAND ----------

schema = StructType([
    StructField("userId",IntegerType(),True),
    StructField("movideId",IntegerType(),True),
    StructField("rating",FloatType(),True),
    StructField("timestamp",LongType(),True)
])

# COMMAND ----------

ratings_df = spark.read\
        .option("Header",True)\
        .option("sep",",")\
        .format("csv")\
        .schema(schema)\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/movie_lens/ml-25m/ratings.csv")

# COMMAND ----------

ratings_df.display()

# COMMAND ----------

agg_df = ratings_df.groupBy("rating","movideId").agg(
                count("rating").alias("count"),
                min("rating").alias("min"),
                max("rating").alias("max")
)

agg_df.display()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("rating", "movieId")
ratings_df.withColumn("row_number", row_number().over(window_spec)).display()

# COMMAND ----------

window_spect = window.partitionBy("rating").orderBy("movieId")
rating_df.withColum("rn", row_number().over(window_spec))

# COMMAND ----------

ratings_df.groupBy("rating").count().display()

# COMMAND ----------

ratings_df.write.mode("overwrite").saveAsTable("hive_metastore.demo.ratings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.demo.ratings

# COMMAND ----------



# COMMAND ----------




