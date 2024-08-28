from pyspark.sql import SparkSession

# Initialize Spark session with additional configuration
spark = SparkSession.builder \
    .appName("Simple CSV Write Test") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Create a sample DataFrame
data = [("John", 28), ("Anna", 23), ("Mike", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Save the DataFrame to local files in CSV format with overwrite mode
df.write.mode("overwrite").csv("output/sample_data", header=True)

# Stop the Spark session
spark.stop()