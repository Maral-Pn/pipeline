from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType


class Transformer():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def flattenAddress(self, dataframe: DataFrame,
                       address_schema: StructType) -> DataFrame:
        df = dataframe.withColumn("adr", F.from_json(F.col("Address"), address_schema))
        return df

    def flattenCitizenship(self, dataframe: DataFrame,
                           citizenship_schema: StructType) -> DataFrame:
        df = (dataframe
              .withColumn("ctz", F.from_json(F.col("Citizenship"), citizenship_schema))
              .withColumn("CTZ", F.explode(F.col("ctz"))))
        return df

    def getFinalDataframe(self, dataframe: DataFrame) -> DataFrame:
        df = (dataframe
              .select(
                F.col("Name"),
                F.col("Family"),
                F.col("adr.*"),
                F.col("Certificate"),
                F.col("CTZ.*")
                )
            )

        return df
