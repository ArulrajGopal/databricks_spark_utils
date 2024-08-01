# Databricks notebook source
storage_account_name = "storageaccountewrere2sdf"
storage_account_access_key = ""
container_name = "raw"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/emp_salary.csv")
df.display()
