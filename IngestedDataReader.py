from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("IngestedDataReader").getOrCreate()
    df = spark.read("json")