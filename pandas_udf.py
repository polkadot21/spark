import pyspark
import pandas as pd
from pyspark.shell import spark

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

def cube_series(x : pd.Series) -> pd.Series:
    return x*x*x

if __name__=="__main__":
    df = spark.range(1, 4)
    df.select("id", cube_series(col("id"))).show()
    print(df)
