from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, desc

if __name__ == '__main__':
    spark = SparkSession.builder.appName('reading_from-blog').getOrCreate()
    schema = "Id int, First string, Last string, Url string, Published string, Hits int, Campaigns Array<string>"
    df = spark.read.schema(schema).json('blogs.json')
    # df.show()
    df.select(expr("Id"), expr("First"), col("Last"), "Url", "Published", expr("Hits * 2"), col("Hits")*2).show()
    big_hitters = df.filter(expr("Hits > 10000")).show()

    df.orderBy(desc(col("Hits"))).show()