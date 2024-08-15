from schema_type import schema_type
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class schema_repository:
    def __init__(self):
        pass
    def get(self, type: schema_type) -> StructType:
        match type:
            case schema_type.ADDRESS:
                schema = StructType([
                    StructField("Unit", IntegerType(), True),
                    StructField("Street No", IntegerType(), True),
                    StructField("Street Name", StringType(), True),
                    StructField("Suburb", StringType(), True),
                    StructField("State", StringType(), True),
                    StructField("Postal Code", IntegerType(), True)
                ])
            case _:
                raise NotImplemented
        return schema

