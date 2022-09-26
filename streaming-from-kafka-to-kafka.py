from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName('streaming-from-kafka-to-kafka').master("local").getOrCreate()
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "input") \
        .option("includeHeaders", "true") \
        .load()
    words = lines.select(split(col('value'), '\\s').alias('word'))
    counts = words.groupBy('word').count()

    checkpointDir = 'checkpoint-dir'
    ds = counts\
        .selectExpr("cast(word as string) as key", "cast(count as string) as value") \
        .writeStream \
        .format("kafka") \
        .outputMode('update') \
        .option('checkpointLocation', checkpointDir) \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "output") \
        .start()
    ds.awaitTermination()
    # streamingQuery = (counts.writeStream.format('kafka').outputMode('complete')
    #                   .option('checkpointLocation', checkpointDir).option('topic', 'output')
    #                   .start())
    # streamingQuery.awaitTermination()
