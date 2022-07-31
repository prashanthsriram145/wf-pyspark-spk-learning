from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StructField, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('higher-order-functions').getOrCreate()
    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    tc = spark.createDataFrame(t_list, schema)
    tc.createOrReplaceTempView("tc")

    spark.sql("""
    SELECT celsius, 
     transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit 
      FROM tC
    """).show()

    spark.sql("""
    SELECT celsius, 
     filter(celsius, t -> t > 38) as high 
      FROM tC
    """).show()

    spark.sql("""
    SELECT celsius, 
           exists(celsius, t -> t = 38) as threshold
      FROM tC
    """).show()

    spark.sql("""
    SELECT celsius, 
           reduce(
              celsius, 
              0, 
              (t, acc) -> t + acc, 
              acc -> (acc div size(celsius) * 9 div 5) + 32
            ) as avgFahrenheit 
      FROM tC
    """).show()
