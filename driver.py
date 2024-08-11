import pyspark as ps
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    print("Hello World!")

    spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

    address1 = """
    {
    "Unit": 13,
    "Street No": 1,
    "Street Name": "Citrus Avenue",
    "Suburb": "Hornsby",
    "State": "NSW",
    "Postal Code": 2077
    }"""

    address2 = """
    {
    "Unit": 612,
    "Street No": 3,
    "Street Name": "Herbert",
    "Suburb": "St Leonards",
    "State": "NSW",
    "Postal Code": 2065
    }"""

    data = [["Maral", "Pour", address1],
            ["Hossein", "Bakh", address2]]

    # giving column names of dataframe
    columns = ["Name", "Family", "Address"]

    # creating a dataframe
    dataframe = spark.createDataFrame(data, columns)

    # show data frame
    dataframe.show(truncate=False)

    schema = StructType([
        StructField("Unit", IntegerType(), True),
        StructField("Street No", IntegerType(), True),
        StructField("Street Name", StringType(), True),
        StructField("Suburb", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", IntegerType(), True)
    ])

    df = (dataframe
          .withColumn("adr", F.from_json(F.col("Address"), schema))
          .select(F.col("Name"), F.col("Family"), F.col("adr.*")))

    df.show(truncate=False)
