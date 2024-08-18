from pyspark.sql import SparkSession, DataFrame
class InputOutput():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def getDummyData(self) -> DataFrame:
        address1 = """
        {
            "Unit": 13,
            "Street No": 1,
            "Street Name": "Citrus Avenue",
            "Suburb": "Hornsby",
            "State": "NSW",
            "Postal Code": 2077
        }"""

        address2 = """
        {
            "Unit": 612,
            "Street No": 3,
            "Street Name": "Herbert",
            "Suburb": "St Leonards",
            "State": "NSW",
            "Postal Code": 2065
        }"""

        citizenship1 = """
        [
            {
                "Country": "Iran",
                "From Date": "1993-05-31",
                "Type": "BIRTH"
            }
        ]
        """

        citizenship2 = """
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
        """

        data = [["Maral", "Pourdayan", address1, citizenship1, ["Databricks Data Engineer", "Databricks Spark Developer"]],
                ["Hossein", "Bakhtiari", address2, citizenship2,
                 ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

        columns = ["Name", "Family", "Address", "Citizenship", "Certificate"]

        dataframe = self.spark.createDataFrame(data, columns)

        return dataframe

    def getParquet(self) -> DataFrame:
        pass