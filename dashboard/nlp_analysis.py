from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from transformers import pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("OlympicNLP").getOrCreate()

# Initialize BERT sentiment pipeline
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# UDF for sentiment analysis
def analyze_sentiment(bio):
    if bio:
        result = sentiment_analyzer(bio)[0]
        return result["label"], result["score"]
    return "NEUTRAL", 0.0

sentiment_udf = udf(analyze_sentiment, StructType([
    StructField("sentiment_label", StringType()),
    StructField("sentiment_score", FloatType())
]))

def nlp_analysis(input_table="staging_athletes"):
    # Read from Synapse
    df = spark.read.format("com.databricks.spark.sql"). \
        option("url", "jdbc:sqlserver://your-synapse-workspace.sql.azuresynapse.net:1433;database=olympics_db"). \
        option("dbtable", input_table). \
        option("user", "your-username"). \
        option("password", "your-password"). \
        load()
    
    # Apply sentiment analysis
    df_with_sentiment = df.withColumn("sentiment", sentiment_udf(df.bio)). \
        select("athlete_id", "name", "bio", "medals", "team", "sentiment.sentiment_label", "sentiment.sentiment_score")
    
    # Write back to Synapse
    df_with_sentiment.write.option("format", "com.databricks.spark.sql"). \
        option("url", "jdbc:sqlserver://your-synapse-workspace.sql.azuresynapse.net:1433;database=olympics_db"). \
        option("dbtable", "athletes_with_sentiment"). \
        option("user", "your-username"). \
        option("password", "your-password"). \
        mode("overwrite").save()
    
    print("Completed NLP analysis and saved to Synapse")

if __name__ == "__main__":
    nlp_analysis()