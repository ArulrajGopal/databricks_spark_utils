from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

def flatten_and_explode(df):
    """
    Recursively flattens all struct fields and explodes arrays.
    
    Args:
    df (DataFrame): Input Spark DataFrame
    
    Returns:
    DataFrame: Fully flattened and exploded DataFrame
    """
    while True:
        flat_cols = []
        has_nested = False  # Flag to check if there are more nested fields

        for field in df.schema.fields:
            field_name = field.name
            
            if isinstance(field.dataType, StructType):  # If Struct, flatten it
                has_nested = True
                for sub_field in field.dataType.fields:
                    flat_cols.append(col(f"{field_name}.{sub_field.name}").alias(f"{field_name}_{sub_field.name}"))
            elif isinstance(field.dataType, ArrayType):  # If Array, explode it
                has_nested = True
                df = df.withColumn(field_name, explode(col(field_name)))
                flat_cols.append(col(field_name))
            else:
                flat_cols.append(col(field_name))
        
        df = df.select(*flat_cols)  # Apply flattening
        
        if not has_nested:  # Stop if no more nested fields
            break
    
    return df
