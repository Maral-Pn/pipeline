from core.schema.my_types import SchemaEnum
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, ArrayType


class SchemaRepository:
    def __init__(self):
        pass

    def getSchema(self, type: SchemaEnum) -> StructType:
        match type:
            case SchemaEnum.ADDRESS:
                schema = StructType([
                    StructField("Unit", LongType(), True),
                    StructField("Street No", LongType(), True),
                    StructField("Street Name", StringType(), True),
                    StructField("Suburb", StringType(), True),
                    StructField("State", StringType(), True),
                    StructField("Postal Code", LongType(), True)
                ])
            case SchemaEnum.CITIZENSHIP:
                inner_schema = StructType([
                    StructField("Country", StringType(), True),
                    StructField("From Date", DateType(), True),
                    StructField("Type", StringType(), True),
                ])

                schema = ArrayType(inner_schema, True)
            case _:
                raise NotImplemented
        return schema
