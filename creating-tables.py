from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('creating-using-tables').getOrCreate()
    spark.catalog.listDatabases()
    spark.sql("create database if not exists learning_spark_db")
    spark.sql("use learning_spark_db")

    spark.sql("create table if not exists managed_flights_table(date string, delay int, distance int, origin string, destination string)")
    schema = "data string, delay int, distance int, origin string, destination string"
    df = spark.read.schema(schema).option('header', 'true').csv("departuredelays.csv")
    df.write.mode("overwrite").saveAsTable("managed_flights_table")

    spark.sql("select * from learning_spark_db.managed_flights_table").show(5)
