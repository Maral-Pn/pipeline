from pyspark.sql import SparkSession, dataframe
class InputOutput():
    def __init__(self, spark: SparkSession):
        self.spark = spark
    def getDummyData(self) -> dataframe:
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

        data = [["Maral", "Pourdayan", address1, ["Databricks Data Engineer", "Databricks Spark Developer"]],
                ["Hossein", "Bakhtiari", address2,
                 ["Google Cloud Platform Professional Data Engineer", "Azure Data Engineer"]]]

        # giving column names of dataframe
        columns = ["Name", "Family", "Address", "Certificate"]

        # creating a dataframe
        dataframe = self.spark.createDataFrame(data, columns)

        return dataframe

    def getParquet(self) -> dataframe:
        pass