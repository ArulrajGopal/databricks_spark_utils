# Databricks notebook source
# DBTITLE 1,loading necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta import DeltaTable

# COMMAND ----------

# DBTITLE 1,establishing adls connectivity
spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "<account_key>"
)

# COMMAND ----------

# DBTITLE 1,reading full file and load it into table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("emp_city", StringType(), True)
])

emp_full_df = spark.read\
        .format("csv")\
        .schema(schema)\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/scd_implementations/emp_details.csv")


emp_full_df.write.mode("overwrite").saveAsTable("emp_details")


# COMMAND ----------

# DBTITLE 1,reading delta file
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("emp_city", StringType(), True)
])

emp_delta_df = spark.read\
        .format("csv")\
        .schema(schema)\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/scd_implementations/emp_details_delta.csv")


dt = DeltaTable.forName(spark, "spark_catalog.default.emp_details")

dt.alias("tgt").merge(emp_delta_df.alias("src"), "tgt.id=src.id")\
                .whenNotMatchedInsertAll()\
                .whenMatchedUpdate(set = {
                    "name": "src.name",
                    "emp_city": "src.emp_city"
                })\
                .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_details

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history spark_catalog.default.emp_details
