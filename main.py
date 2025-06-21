from pyspark.sql import SparkSession

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("NYC Yellow Taxi Analysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Step 2: Read the Parquet file
df = spark.read.parquet("yellow_tripdata_2023-01.parquet")

# Step 3: Show basic schema and sample rows
df.printSchema()
df.show(5)

# Step 4: Basic analysis — example: total trips and average trip distance
df.select("trip_distance").describe().show()

# Step 5: Filter long trips (just an example)
df.filter(df["trip_distance"] > 10).show(5)

# Remove trips with zero or negative distance
df_clean = df.filter(df.trip_distance > 0)


# Step 6: Stop the session (optional)
spark.stop()
