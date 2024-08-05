# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #Dataframe preparation

# COMMAND ----------

record_1 = [1,'A','arul','cricket']
record_2 = [2,'A','sekar','chess']
record_3 = [3,'A','kumar','tennis']
record_4 = [1,'B', 'ganesh','football']
record_5 = [2,'B','vinoth','volleyball']
record_6 = [3,'B','Ravi','hockey']

record_6 = [1, 'A','Engineer']
record_7 = [2, 'A', 'doctor']
record_8 = [2,'B', 'lawyer']

list1 = [record_1, record_2, record_3,record_4,record_5]
list2 = [record_6, record_7, record_8]

df_schema = StructType(fields=[StructField("sr_no", IntegerType(), False),
                               StructField("section", StringType(), False),
                                StructField("name", StringType(), True),
                               StructField("fav_game", StringType(), True)    
])

df_2_schema = StructType(fields=[StructField("sr_no", IntegerType(), False),
                                 StructField("section", StringType(), False),
                                StructField("profession", StringType(), True),     
])

df = spark.createDataFrame(list1, df_schema)
df_2 = spark.createDataFrame(list2, df_2_schema)

# COMMAND ----------

df.show()
df_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Joining with Multiple condition

# COMMAND ----------

joined_df = df.alias('LH')\
                .join(df_2.alias('RH'), (col('LH.sr_no') == col('RH.sr_no')) & (col('LH.section') == col('RH.section')) , 'left')\
                .select('LH.*','RH.profession')

joined_df.show()

# COMMAND ----------

joined_df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Broadcast join

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

large_df = df
small_df = df_2

result_df = large_df.alias("LH").join(broadcast(small_df.alias("RH")), (col('LH.sr_no') == col('RH.sr_no')) & (col('LH.section') == col('RH.section')), "left")
result_df.display()
# Note: - this is not large enough to broadcast, but for demo purpose, it has been done.

result_df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Auto Broadcast

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# To disable autoBroadcastJoin >> set -1
# By default it is 10485760 i.e. 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
