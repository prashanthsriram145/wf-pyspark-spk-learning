from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import time

if __name__ == "__main__":
    spark = SparkSession.builder.appName('flight-delays-finder').getOrCreate()
    schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"
    df = spark.read.schema(schema).option("header", "true").csv("departuredelays.csv")
    df.createOrReplaceTempView("flights")
    spark.sql("select * from flights where distance > 1000 order by distance desc").show(5)
    df.select("*").filter("distance > 1000").orderBy(desc(col("distance"))).show(5)

    spark.sql("select * from flights where delay > 120 and origin='SFO' and destination='ORD' order by delay desc").show(5)
    df.select("*").filter("delay > 120 and origin='SFO' and destination='ORD'").orderBy(desc(col("delay"))).show(5)

    df.write.mode("overwrite").csv("departuredelays_df.csv")

    time.sleep(100)