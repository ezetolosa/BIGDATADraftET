from pyspark.sql import SparkSession
import os

# Configure Spark environment
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = r".\venv\Scripts\python.exe"

def test_spark():
    """Test Spark setup with minimal configuration"""
    try:
        # Create Spark session with minimal config
        spark = SparkSession.builder \
            .appName("Test") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .getOrCreate()
            
        # Create and show test data
        df = spark.createDataFrame([("test",)], ["col1"])
        df.show()
        
        spark.stop()
        print("✅ Spark test successful!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    test_spark()