# Column renaming

data = [["Arpit", "ENGG", 8.5],["Anand", "PG",6.7],["Maz","MEDICAL",9.2]]
columns = ["Name", "Profession", "CGPA"]
df = spark.createDataFrame(data, columns)
dict = {'Name':'Candidate_name','Profession':'Career','CGPA':'Marks'}
list = []
for i, j in dict.items():
  list.append(f"{i} as {j}")

df.selectExpr(*list).show()



# Casting
from pyspark.sql.types import  StructType,  StructField, StringType, IntegerType
from pyspark.sql.functions import lit

df_schema = StructType(fields=[StructField("sr_no", StringType(), False),
                                StructField("name", StringType(), True),
                                StructField("age", StringType(), True),
                                StructField("fav_sport", StringType(), True)])
                  
list = [
  (1, 'Arul',23,'football'),
  (2,'Sekar',34,'cricket'),
  (3,'Vinoth',33,'chess'),
  (4,'Ravi',30,'tennis')]

df = spark.createDataFrame(list, df_schema)


df.show()
df.printSchema()
#direct casting
df_1= df.select(df.age.cast("int"))
#casting without alias
df_2 = df.selectExpr('cast(age as INT)')
#casting with alias
df_3 = df.selectExpr('cast(age as INT) as new_age')
#casting with new column as null
df_4 = df.withColumn('new_age',lit(None))
#casting with new column as null using selectExpr
df_5 = df.selectExpr('cast(null as INT) as new_age')


df_1.show()
df_1.printSchema()
df_2.show()
df_2.printSchema()
df_3.show()
df_3.printSchema()
df_4.show()
df_4.printSchema()
df_5.show()
df_5.printSchema()




