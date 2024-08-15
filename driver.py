import pyspark as ps
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from schema_repository import schema_repository
from schema_type import schema_type

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

    data = [["Maral", "Pourdayan", address1, ["Databricks Data Engineer","Databricks Spark Developer"]],
            ["Hossein", "Bakhtiari", address2, ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

    # giving column names of dataframe
    columns = ["Name", "Family", "Address", "Certificate"]

    # creating a dataframe
    dataframe = spark.createDataFrame(data, columns)

    # show data frame
    dataframe.show(truncate=False)

    repo = schema_repository()

    schema = repo.get(schema_type.ADDRESS)

    df = (dataframe
          .withColumn("adr", F.from_json(F.col("Address"), schema))
          .select(F.col("Name"), F.col("Family"), F.col("adr.*"), F.explode(F.col("Certificate"))))

    df.show(truncate=False)
