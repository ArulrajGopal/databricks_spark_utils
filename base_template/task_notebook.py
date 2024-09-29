# Databricks notebook source
# MAGIC %md
# MAGIC #Reading Secrets

# COMMAND ----------

storage_account_access_key= ""
jdbcPassword = ""

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing libraries 

# COMMAND ----------

from delta import DeltaTable
from pyspark.sql.functions import col,to_date, rank, when, lit, current_date
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



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Percentage hike from previous salary
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
