from pyspark.sql import SparkSession, DataFrame
class InputOutput():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_dummy_address(self) -> list[str]:
        address = ["""
                {
                    "Unit": 1,
                    "Street No": 41,
                    "Street Name": "Evan Avenue",
                    "Suburb": "Mosman",
                    "State": "NSW",
                    "Postal Code": 1234
                }""",

        """
                {
                    "Unit": 54,
                    "Street No": 87,
                    "Street Name": "Dave",
                    "Suburb": "Darling Point",
                    "State": "NSW",
                    "Postal Code": 2341
                }"""]
        return address

    def get_dummy_citizenship(self):
        return ["""
        [
            {
                "Country": "Spain",
                "From Date": "1990-01-11",
                "Type": "BIRTH"
            }
        ]
        """
        ,"""
            [
                {
                    "Country": "Portugal",
                    "From Date": "1980-10-20",
                    "Type": "BIRTH"
                },
                {
                    "Country": "Australia",
                    "From Date": "2020-03-30",
                    "Type": "CONFERRAL"
                }
            ]
            """]
    def get_dummy_data(self) -> DataFrame:
        address = self.get_dummy_address()

        citizenship = self.get_dummy_citizenship()

        data = [["Maral", "Pourdayan", address[0], citizenship[0], ["Databricks Data Engineer", "Databricks Spark Developer"]],
                ["Hossein", "Bakhtiari", address[1], citizenship[1],
                 ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

        columns = ["Name", "Family", "Address", "Citizenship", "Certificate"]

        dataframe = self.spark.createDataFrame(data, columns)

        return dataframe

    def get_from_parquet(self) -> DataFrame:
        pass
