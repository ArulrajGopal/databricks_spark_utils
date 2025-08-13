from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

account_key = ""

spark.conf.set(
    "fs.azure.account.key.arulrajgopalshare.blob.core.windows.net",
    account_key
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("middle_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_dt", TimestampType(), True),
    StructField("ssn", StringType(), True),
    StructField("salary", IntegerType(), True)
])


df = spark.read.format("csv") \
    .schema(schema) \
    .load("wasbs://kaniniwitharul@arulrajgopalshare.blob.core.windows.net/people_csv/people.csv")



df.limit(10).show()

