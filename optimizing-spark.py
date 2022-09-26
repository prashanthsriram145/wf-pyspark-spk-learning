from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.appName('optimizing-spark').master("local").getOrCreate()
    df = spark.range(1 * 10000000).toDF("id").withColumn("square", col('id') * col('id'))
    df.cache()
    print(df.count())

    print(df.count())
    df.show(10)