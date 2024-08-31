from pyspark.sql import SparkSession, DataFrame
class InputOutput():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def getDummyAddress(self) -> list[str]:
        address = ["""
                {
                    "Unit": 13,
                    "Street No": 1,
                    "Street Name": "Citrus Avenue",
                    "Suburb": "Hornsby",
                    "State": "NSW",
                    "Postal Code": 2077
                }""",

        """
                {
                    "Unit": 612,
                    "Street No": 3,
                    "Street Name": "Herbert",
                    "Suburb": "St Leonards",
                    "State": "NSW",
                    "Postal Code": 2065
                }"""]
        return address

    def getDummyCitizenship(self):
        return ["""
        [
            {
                "Country": "Iran",
                "From Date": "1993-05-31",
                "Type": "BIRTH"
            }
        ]
        """
        ,"""
            [
                {
                    "Country": "Iran",
                    "From Date": "1987-12-05",
                    "Type": "BIRTH"
                },
                {
                    "Country": "Australia",
                    "From Date": "2024-10-01",
                    "Type": "CONFERRAL"
                }
            ]
            """]
    def getDummyData(self) -> DataFrame:
        address = self.getDummyAddress()

        citizenship = self.getDummyCitizenship()

        data = [["Maral", "Pourdayan", address[0], citizenship[0], ["Databricks Data Engineer", "Databricks Spark Developer"]],
                ["Hossein", "Bakhtiari", address[1], citizenship[1],
                 ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

        columns = ["Name", "Family", "Address", "Citizenship", "Certificate"]

        dataframe = self.spark.createDataFrame(data, columns)

        return dataframe

    def getParquet(self) -> DataFrame:
        pass