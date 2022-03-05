from __future__ import print_function

import sys
from pyspark.sql import SparkSession

def load_df():
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    # get the data set file name
    df_file = sys.argv[1]

    # read the file into a Spark DataFrame
    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(df_file))


    df_500 = df.filter(df['New Cases'] >= 500)
    df_500.show()

    #df.show(n=5, truncate=False)
    print("Total Rows = %d" % (df_500.count()))

if __name__ == "__main__":
    load_df()