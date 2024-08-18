from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType


class Transformer():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transformDataframe(self, dataframe: DataFrame, address_schema: StructType, citizenship_schema: StructType) -> DataFrame:
        df = (dataframe
              .withColumn("adr", F.from_json(F.col("Address"), address_schema))
              .withColumn("ctz", F.from_json(F.col("Citizenship"), citizenship_schema))
              .withColumn("CTZ", F.explode(F.col("ctz")))
              .select(
                F.col("Name"),
                F.col("Family"),
                F.col("adr.*"),
                F.col("Certificate"),
                F.col("CTZ.Country"),
                F.col("CTZ.`From Date`"),
                F.col("CTZ.Type")
                )
            )

        return df
