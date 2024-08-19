import pyspark as ps
import pyspark.sql
from pyspark.sql.dataframe import DataFrame
from code.core.io.input_output import InputOutput
from code.core.schema.schema_repository import SchemaRepository
from code.core.schema.my_types import SchemaEnum
from code.core.transform.transformer import Transformer

class Pipeline:

    def initialiseSpark(self, master: str) -> pyspark.sql.SparkSession:
        match master:
            case "local":
                spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
            case m:
                spark = ps.sql.SparkSession.builder.appName("Test").master(m).getOrCreate()
        self.spark = spark

    def runPipeline(self):
        df = self.read()
        df_final = self.transform(df)
        self.write(df_final)

    def read(self) -> DataFrame:
        dataframe = InputOutput(self.spark).getDummyData()
        return dataframe

    def transform(self, dataframe: DataFrame) -> DataFrame:
        repo = SchemaRepository()

        address_schema = repo.getSchema(SchemaEnum.ADDRESS)
        citizenship_schema = repo.getSchema(SchemaEnum.CITIZENSHIP)

        transformer = Transformer(self.spark)
        df_adr = transformer.flattenAddress(dataframe, address_schema)
        df_ctz = transformer.flattenCitizenship(df_adr, citizenship_schema)

        df = transformer.getFinalDataframe(df_ctz)

        return df

    def write(self, dataframe: DataFrame):
        dataframe.write.partitionBy("Country").json("people")
