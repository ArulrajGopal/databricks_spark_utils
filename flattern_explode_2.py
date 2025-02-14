from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame

def flatten_structs(df: DataFrame) -> DataFrame:
    """
    Recursively flattens all nested structs in a PySpark DataFrame.
    """
    while any(field.dataType.typeName() == "struct" for field in df.schema.fields):
        df = df.select(
            *[
                col(f"{col_name}.{sub_field.name}").alias(f"{col_name}_{sub_field.name}")
                if field.dataType.typeName() == "struct" else col(col_name)
                for field in df.schema.fields
                for col_name in [field.name]
                for sub_field in (field.dataType.fields if field.dataType.typeName() == "struct" else [field])
            ]
        )
    return df

def explode_arrays(df: DataFrame) -> DataFrame:
    """
    Recursively explodes all arrays in a PySpark DataFrame.
    """
    while any(field.dataType.typeName() == "array" for field in df.schema.fields):
        for field in df.schema.fields:
            if field.dataType.typeName() == "array":
                df = df.withColumn(field.name, explode(col(field.name)))
    return df

def normalize_dataframe(df: DataFrame) -> DataFrame:
    """
    Flattens structs and explodes arrays recursively in a PySpark DataFrame.
    """
    df = flatten_structs(df)
    df = explode_arrays(df)
    return df
