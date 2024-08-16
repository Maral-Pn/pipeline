import pyspark as ps
import pyspark.sql
from input_output import InputOutput
from schema_repository import SchemaRepository
from my_types import SchemaEnum
from transformer import Transformer

def initialise_spark(master: str) -> pyspark.sql.SparkSession:
    match master:
        case "local":
            spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
        case m:
            spark = ps.sql.SparkSession.builder.appName("Test").master(m).getOrCreate()
    return spark


if __name__ == "__main__":

    spark = initialise_spark("local")

    dataframe = InputOutput(spark).getDummyData()

    dataframe.show(truncate=False)

    repo = SchemaRepository()

    schema = repo.get(SchemaEnum.ADDRESS)

    df = Transformer(spark).transformDataframe(dataframe, schema)

    df.show(truncate=False)
