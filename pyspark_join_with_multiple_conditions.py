# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col

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

list = [record_1, record_2, record_3,record_4,record_5,record_6]
list_2 = [record_6, record_7, record_8]

# COMMAND ----------

df_schema = StructType(fields=[StructField("sr_no", IntegerType(), False),
                               StructField("section", StringType(), False),
                                StructField("name", StringType(), True),
                               StructField("fav_game", StringType(), True)    
])

df_2_schema = StructType(fields=[StructField("sr_no", IntegerType(), False),
                                 StructField("section", StringType(), False),
                                StructField("profession", StringType(), True),     
])

# COMMAND ----------

df = spark.createDataFrame(list, df_schema)
df_2 = spark.createDataFrame(list_2, df_2_schema)

# COMMAND ----------

df.show()
df_2.show()

# COMMAND ----------

joined_df = df.alias('LH')\
                .join(df_2.alias('RH'), (col('LH.sr_no') == col('RH.sr_no')) & (col('LH.section') == col('RH.section')) , 'inner')\
                .select('LH.*','RH.profession')

joined_df.show()
