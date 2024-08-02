# Databricks notebook source
# MAGIC %md
# MAGIC #Reading Secrets

# COMMAND ----------

# MAGIC %run "./secrets"

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing libraries 

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col,to_date, rank, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Read/transform/load into ADLS

# COMMAND ----------

storage_account_name = "test8139"
container_name = "raw"
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ##emp_details

# COMMAND ----------

emp_details_df = spark.read\
    .format("csv")\
    .option("Sep","~")\
    .option("header", True)\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_details.csv")

# COMMAND ----------

emp_details_df.write.mode("overwrite").format("delta").saveAsTable("spark_catalog.default.emp_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ##emp_salary_designation

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

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_designation

# COMMAND ----------

# MAGIC %md
# MAGIC ##emp_role

# COMMAND ----------

emp_role = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_role.csv")

# COMMAND ----------

emp_role.write.format("delta").mode("overwrite").saveAsTable("spark_catalog.default.emp_role")

# COMMAND ----------

# MAGIC %md
# MAGIC #Read from Azure SQL Server

# COMMAND ----------

jdbcHostname = "arulrajtestserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "test_database"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUsername = "Arulraj"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

# COMMAND ----------

read_from_sql_df= spark.read.format("jdbc")\
                .option("url",jdbcUrl)\
                .option("dbtable","mytable")\
                .load()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD-1 implementation

# COMMAND ----------

#reading the delta df 
emp_details_delta_df = spark.read\
    .format("csv")\
    .option("Sep","~")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_details_delta.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_details

# COMMAND ----------

#source_df >> emp_details_df
#target table >> spark_catalog.default.emp_details
dt = DeltaTable.forName(spark, "spark_catalog.default.emp_details")

update_set = {
    "name": "src.name",
    "emp_city": "src.emp_city"
}

dt.alias("tgt")\
    .merge(emp_details_delta_df.alias("src"),"tgt.id = src.id")\
    .whenMatchedUpdate(set = update_set)\
    .whenNotMatchedInsertAll()\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_details
# MAGIC order by id

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD-2 implementation

# COMMAND ----------

#reading the delta
emp_salary_designation_delta_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary_designation_delta.csv")

# COMMAND ----------

emp_salary_designation_delta_df.display()

# COMMAND ----------

dt = DeltaTable.forName(spark, "spark_catalog.default.emp_designation")

dt.alias("tgt").merge()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_catalog.default.emp_designation

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# Syntax for broad cast join?
# Join_cond = (
# (col(“A”) == col(“B”)) & 
# (col(“C”) == col(“D”)) &
# )
# New_df = df.alias(“LH”).join(broadcast(df1).alias(“RH”), Join_cond
# , “left”)
# ---------------------------------------------------------------------------------
# Syntax for row number in pyspark?
# Wspec = Window.partitionBy(“col1”,”col2”,”Col3”)\	
# .orderBy(desc(“col3”), (“col4”)
# New_df = df.withColumn(“RN”, row_number ().over(Wspec))
# ---------------------------------------------------------------------------------



# Syntax upsert?
# abc_delta_tab = DeltaTable.forName(spark, table_name)
# abc_delta_tab.alias(“LH”).merge(1)\
# .whenMatchedInser(2)\
# .whenMatchedUpdate(3)\
# .execute(4)
# 1 >> source_df.alias(“RH”), join_cond
# 2 >>  cond
# 3 >> cond, set = dict

