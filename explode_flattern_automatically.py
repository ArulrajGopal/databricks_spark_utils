from pyspark.sql.functions import col,explode_outer
from pyspark.sql.types import StructType

def flatten_schema(df, prefix=""):
    flat_cols = []
    
    for field in df.schema.fields:
        col_name = f"{prefix}{field.name}" if prefix else field.name  # Maintain hierarchy in names
        
        if isinstance(field.dataType, StructType):  # If field is a StructType, recurse
            for sub_field in field.dataType.fields:
                flat_cols.append(col(f"{col_name}.{sub_field.name}").alias(f"{col_name}_{sub_field.name}"))
        else:
            flat_cols.append(col(col_name))
    
    return df.select(*flat_cols)

def recursive_flatten(df):
    while any(isinstance(field.dataType, StructType) for field in df.schema.fields):
        df = flatten_schema(df)
    
    return df

def explode_array(df,column_name):
    return df.withColumn(column_name, explode_outer(df[column_name]))


