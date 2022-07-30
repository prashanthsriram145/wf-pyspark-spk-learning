from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == '__main__':
    spark = SparkSession.builder.appName("finding-avg").getOrCreate()
    df = spark.createDataFrame([('Brooke', 20), ('Denny', 31), ('Jules', 30), ('TD', 35),
                           ('Brooke', 25)], "name string, age int")
    avgs_df = df.groupBy("name").agg(avg("age"))
    avgs_df.show()
