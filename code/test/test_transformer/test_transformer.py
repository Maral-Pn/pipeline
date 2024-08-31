from code.core.transform.transformer import Transformer
from code.core.io.input_output import InputOutput
from code.core.schema.schema_repository import SchemaRepository
from code.core.schema.my_types import SchemaEnum
import pyspark.sql.functions as F

from code.test.test_base import TestBase


class TestTransformer(TestBase):

    def setUp(self):
        super().setUp()
        self.transformer = Transformer(self.spark)
        self.io = InputOutput(self.spark)

    def test_flatten_address(self):
        df = self.io.getDummyData()
        citizenship = self.io.getDummyCitizenship()

        data = [
            ["Maral", "Pourdayan", 13, 1, "Citrus Avenue", "Hornsby", "NSW", 2077, citizenship[0], ["Databricks Data Engineer", "Databricks Spark Developer"]],
            ["Hossein", "Bakhtiari", 612, 3, "Herbert", "St Leonards", "NSW", 2065, citizenship[1],
             ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

        columns = ["Name", "Family", "Unit", "Street No", "Street Name", "Suburb", "State", "Postal Code", "Citizenship", "Certificate"]

        dataframe = self.spark.createDataFrame(data, columns)
        address_schema = SchemaRepository().getSchema(SchemaEnum.ADDRESS)
        desired_df = (self.transformer.flattenAddress(df, address_schema).drop(F.col("Address"))
                      .select(F.col("Name"),
                              F.col("Family"),
                              F.col("adr.Unit").alias("Unit"),
                              F.col("adr.Street No").alias("Street No"),
                              F.col("adr.Street Name").alias("Street Name"),
                              F.col("adr.Suburb").alias("Suburb"),
                              F.col("adr.State").alias("State"),
                              F.col("adr.Postal Code").alias("Postal Code"),
                              F.col("Citizenship"),
                              F.col("Certificate"))
                      )
        self.assertDataFrameEqual(dataframe, desired_df)

    def test_flatten_citizenship(self):
        pass

    def test_get_final_dataframe(self):
        pass

if __name__ == '__main__':
    unittest.main()