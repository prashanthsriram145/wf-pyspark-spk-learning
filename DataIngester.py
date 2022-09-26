from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

if __name__ == '__main__':
    spark = SparkSession.builder.appName('DataIngester-1').master("local").getOrCreate()
    recipes = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "backeryRecipes") \
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .load()

    checkpointDir = 'checkpoint-dir-1'
    ds = recipes \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("json") \
        .outputMode('append') \
        .option('checkpointLocation', checkpointDir) \
        .option("path", "output/json") \
        .start()
    ds.awaitTermination()
    # streamingQuery = (counts.writeStream.format('kafka').outputMode('complete')
    #                   .option('checkpointLocation', checkpointDir).option('topic', 'output')
    #                   .start())
    # streamingQuery.awaitTermination()
