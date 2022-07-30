from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('using-tables').getOrCreate()
    print(spark.catalog.listDatabases())
    spark.sql("use learning_spark_db")
    print(spark.catalog.listTables())
    spark.sql("select * from managed_flights_table").show(5)
    df = spark.table("managed_flights_table")
    df.show(5)