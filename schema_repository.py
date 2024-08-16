from my_types import SchemaEnum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


class SchemaRepository:
    def __init__(self):
        pass

    def get(self, type: SchemaEnum) -> StructType:
        match type:
            case SchemaEnum.ADDRESS:
                schema = StructType([
                    StructField("Unit", IntegerType(), True),
                    StructField("Street No", IntegerType(), True),
                    StructField("Street Name", StringType(), True),
                    StructField("Suburb", StringType(), True),
                    StructField("State", StringType(), True),
                    StructField("Postal Code", IntegerType(), True)
                ])
            case SchemaEnum.CITIZENSHIP:
                schema = StructType([
                    StructField("Country", StringType(), True),
                    StructField("From date", DateType(), True),
                    StructField("Type", StringType(), True),
                ])
            case _:
                raise NotImplemented
        return schema
