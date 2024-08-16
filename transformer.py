from pyspark.sql import SparkSession, dataframe
import pyspark.sql.functions as F
from pyspark.sql.types import StructType

class Transformer():
    def __init__(self, spark: SparkSession):
        self.spark = spark
    def transformDataframe(self, dataframe: dataframe, schema: StructType) -> dataframe:
        df = (dataframe
              .withColumn("adr", F.from_json(F.col("Address"), schema))
              .select(F.col("Name"), F.col("Family"), F.col("adr.*"), F.explode(F.col("Certificate"))))
        return df
