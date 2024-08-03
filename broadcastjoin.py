# COMMAND ----------

# Syntax for broad cast join?
# Join_cond = (
# (col(“A”) == col(“B”)) & 
# (col(“C”) == col(“D”)) &
# )
# New_df = df.alias(“LH”).join(broadcast(df1).alias(“RH”), Join_cond
# , “left”)
