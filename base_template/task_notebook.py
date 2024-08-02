# Databricks notebook source
# MAGIC %md
# MAGIC #Reading Secrets

# COMMAND ----------

# MAGIC %run "./secrets"

# COMMAND ----------

# MAGIC %md
# MAGIC #Read a file from AzureDateLakeStorage

# COMMAND ----------

storage_account_name = "test8139"
container_name = "raw"
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

emp_details_df = spark.read\
    .format("csv")\
    .option("Sep","~")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_details.csv")

# COMMAND ----------

emp_salary_designation_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary_designation.csv")

# COMMAND ----------

emp_role = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_role.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading delta files

# COMMAND ----------

emp_salary_designation_delta_df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary_designation_delta.csv")

# COMMAND ----------

emp_details_df = spark.read\
    .format("csv")\
    .option("Sep","~")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_details_delta.csv")

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
# MAGIC #SCD 2 type implementation

# COMMAND ----------


