from pyspark.sql.functions import col

def flatten_struct(df):
    """Flattens all struct columns in a DataFrame."""
    flat_cols = []
    
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):  # Check if column is a Struct
            for sub_field in field.dataType.fields:
                flat_cols.append(col(f"{field.name}.{sub_field.name}").alias(f"{field.name}_{sub_field.name}"))
        else:
            flat_cols.append(col(field.name))
    
    return df.select(*flat_cols)
