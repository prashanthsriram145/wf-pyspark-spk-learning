from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('reading-writing-parquet').getOrCreate()
    df = spark.read.load("parquet-input")
    df.show(5)
    df.write.mode("overwrite").save("parquet_output")
    spark.sql("use learning_spark_db")
    df.write.mode("overwrite").saveAsTable("managed_flights_parquet")
    spark.sql("select * from managed_flights_parquet").show(5)