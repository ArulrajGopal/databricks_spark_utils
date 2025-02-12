from pyspark.sql.functions import col
from pyspark.sql.types import StructType

def flatten_schema(df, prefix=""):
    """
    Recursively flattens all levels of struct columns in a DataFrame.
    
    Args:
    df (DataFrame): Input Spark DataFrame
    prefix (str): Prefix for column names during flattening
    
    Returns:
    DataFrame: Flattened DataFrame
    """
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
    """
    Recursively applies flattening until no struct fields remain.
    
    Args:
    df (DataFrame): Input Spark DataFrame
    
    Returns:
    DataFrame: Fully flattened DataFrame
    """
    while any(isinstance(field.dataType, StructType) for field in df.schema.fields):
        df = flatten_schema(df)
    
    return df
