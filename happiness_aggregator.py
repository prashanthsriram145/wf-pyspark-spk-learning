from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


def main():
    spark = SparkSession.builder.appName("happiness_aggregator").master("local").getOrCreate()
    df = spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "backeryRecipes") \
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .load()

    happiness_data = df.select(split(df.value, ' ')[0].alias('country'),
                               split(df.value, ' ')[1].alias('region'),
                               split(df.value, ' ')[2].alias('happinessScore'));

    happiness_data.createOrReplaceTempView("happiness_data")

    happiness_aggs = spark.sql(""" select region, avg(happinessScore) from happiness_data group by region """)

    happiness_aggs.writeStream.outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "checkpoint_dir") \
        .start().awaitTermination()


if __name__ == '__main__':
    main()
