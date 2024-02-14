from pyspark.sql.functions import col, lit, concat, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType 
from datetime import datetime

catalog_name = "my_catalog"
schema_name = "my_schema"
catalog_schema_name = catalog_name+"."+schema_name

df = spark.sql(f"show tables from {catalog_schema_name}")
dfl = df.withColumn ("Table", concat (lit (catalog_name), lit("."), col("database"), lit("."), col ("tablename"))) 

df2 = dfl.select("Table")
tbl_1st = df2.rdd.map(lambda x : x.Table).collect()

final_list = []
for tbl in tbl_1st:
	df4=spark.sql(f"desc history (tbl)")\
					.orderBy(desc("version")).select("version").limit(1)\
					.withColumn ("tablename", lit (tbl))
	run_detail = df4.withColumn ("Details", concat(col ("tablename"),lit("#"),col("version")))\ 
					.select("Details")\
					.rdd.map(lambda x : x.Details)\
					.collect()
			
	final_list.append(run_detail)
			
			

schema = StructType([StructField("TableDetails", StringType(), True)])

intiate_df = spark.createDataFrame (final_list, schema)
intiate_df.display()
