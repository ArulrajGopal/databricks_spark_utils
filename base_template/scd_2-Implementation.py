# Databricks notebook source
# MAGIC %md
# MAGIC #Pre-step

# COMMAND ----------

# MAGIC %run "./secrets"

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col,to_date, rank, when, lit, current_date
from pyspark.sql.window import Window

# COMMAND ----------

storage_account_name = "test8139"
container_name = "raw"
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC #FullLoad

# COMMAND ----------

#reading a file into df
emp_salary_designation_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary_designation.csv")

# COMMAND ----------

#data type conversion
date_converted_df = emp_salary_designation_df.withColumn('updated_date', to_date(col('updated_date'), 'dd-MMM-yy').alias('updated_date').cast('date'))

data_type_map = {
  'rec_id': 'int', 
  'emp_id': 'int', 
  'salary': 'int'
  }

cast_col_list = []

for column_schema in date_converted_df.dtypes:
    cast_col_list.append(col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])))

data_type_convereted_df = date_converted_df.select(*cast_col_list)

# COMMAND ----------

window_spec = Window.partitionBy('emp_id').orderBy(col('updated_date').desc())
window_applied_df = data_type_convereted_df.withColumn("rank", rank().over(window_spec))
                                                       
status_active_applied_df = window_applied_df.withColumn("is_active", when(col("rank") == 1, 'Y').otherwise('N'))\
                                    .drop("rank")

# COMMAND ----------

status_active_applied_df.write.mode("overwrite").format("delta").saveAsTable("spark_catalog.default.emp_designation")

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading delta and data conversion

# COMMAND ----------

#reading the delta
emp_salary_designation_delta_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary_designation_delta.csv")

#data type conversion
date_converted_df = emp_salary_designation_delta_df.withColumn('updated_date', to_date(col('updated_date'), 'dd-MMM-yy').alias('updated_date').cast('date'))

data_type_map = {
  'rec_id': 'int', 
  'emp_id': 'int', 
  'salary': 'int'
  }

cast_col_list = []

for column_schema in date_converted_df.dtypes:
    cast_col_list.append(col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])))

data_type_convereted_df = date_converted_df.select(*cast_col_list)

# COMMAND ----------

data_type_convereted_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_designation

# COMMAND ----------

target_df = spark.read.table("spark_catalog.default.emp_designation")

new_key_added_df = data_type_convereted_df.withColumn("new_key",col("emp_id"))

joined_df = target_df.alias("LH").join(new_key_added_df.alias("RH"), col("LH.emp_id")==col("RH.emp_id"),"inner")\
                    .select("RH.*")\
                    .withColumn("new_key",lit(None))

delta_df = new_key_added_df.union(joined_df)\
                    .withColumn("is_active", lit("Y"))


dt = DeltaTable.forName(spark, "spark_catalog.default.emp_designation")

dt.alias("tgt").merge(delta_df.alias("src"), "tgt.emp_id=src.new_key")\
                .whenNotMatchedInsertAll()\
                .whenMatchedUpdate(set = {
                    "updated_date": lit(current_date()),
                    "is_active":lit("N")
                })\
                .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_designation
