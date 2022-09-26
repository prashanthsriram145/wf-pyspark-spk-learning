from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode


def main():
    spark = SparkSession.builder.appName("word-count").master("local").getOrCreate()
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "backeryRecipes") \
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .load()

    words = df.select(explode(split(df.value, ' ')).alias('word'))

    wordCount = words.groupBy('word').count().orderBy('count')

    wordCount.writeStream.format("console")\
        .outputMode("complete")\
        .start().awaitTermination()


if __name__ == '__main__':
    main()
