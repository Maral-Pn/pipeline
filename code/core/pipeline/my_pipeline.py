import pyspark as ps
import pyspark.sql
from pyspark.sql.dataframe import DataFrame
from core.io.input_output import InputOutput
from core.schema.schema_repository import SchemaRepository
from core.schema.my_types import SchemaEnum
from core.transform.transformer import Transformer


class Pipeline:

    def initialise_spark(self, master: str) -> pyspark.sql.SparkSession:
        match master:
            case "local":
                spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
            case m:
                spark = ps.sql.SparkSession.builder.appName("Test").master(m).getOrCreate()
        self.spark = spark

    def run_pipeline(self):
        df = self.read()
        df_final = self.transform(df)
        self.write(df_final)

    def read(self) -> DataFrame:
        dataframe = InputOutput(self.spark).get_dummy_data()
        return dataframe

    def transform(self, dataframe: DataFrame) -> DataFrame:
        dataframe.show(truncate=False)
        repo = SchemaRepository()

        address_schema = repo.get_schema(SchemaEnum.ADDRESS)
        citizenship_schema = repo.get_schema(SchemaEnum.CITIZENSHIP)

        transformer = Transformer(self.spark)
        df_adr = transformer.flatten_address(dataframe, address_schema)

        df_adr.show(truncate=False)
        df_ctz = transformer.flatten_citizenship(df_adr, citizenship_schema)

        df_ctz.show(truncate=False)
        df = transformer.get_final_dataframe(df_ctz)

        df.show(truncate=False)
        return df

    def write(self, dataframe: DataFrame):
        dataframe.write.mode("overwrite").partitionBy("Country").json("people")
