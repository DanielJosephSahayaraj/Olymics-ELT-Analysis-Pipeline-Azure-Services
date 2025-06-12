from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session in Azure Databricks
spark = SparkSession.builder.appName("OlympicELT").getOrCreate()

def transform_data(input_path="wasbs://olympic-data@yourstorage.blob.core.windows.net/raw/olympics_*.csv"):
    # Read from Azure Blob Storage
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Clean and transform data
    df_clean = df.dropna(subset=["athlete_id", "name"]). \
    select("athlete_id", "name", "bio", "medals", "team"). \
        withColumn("medals", col("medals").cast("integer"))
    
    # Aggregate: total medals by team
    team_medals = df_clean.groupBy("team").agg({"medals": "sum"}).alias("total_medals")
    
    # Write to Synapse (staging table)
    team_medals.write.option("format", "com.databricks.spark.sql"). \
        option("url", "jdbc:sqlserver://your-synapse-workspace.sql.azuresynapse.net:1433;database=olympics_db"). \
        option("dbtable", "staging_team_medals"). \
        option("user", "your-username"). \
        option("password", "your-password"). \
        mode("overwrite").save()
    
    print("Transformed data and saved to Synapse")

if __name__ == "__main__":
    transform_data()