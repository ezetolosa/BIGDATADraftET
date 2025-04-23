import pytest
from pyspark.sql import SparkSession
from src.data_processing import DataProcessor

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestSoccer").getOrCreate()

def test_data_validation(spark):
    processor = DataProcessor(spark)
    df = spark.createDataFrame([], "date string, home_team_goal int")
    with pytest.raises(ValueError):
        processor.validate_data(df)