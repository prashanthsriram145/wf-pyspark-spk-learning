import random
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, exp, asc

if __name__ == '__main__':
    spark = SparkSession.builder.appName('using-sortmergejoin-optimized').getOrCreate()
    usersDf = spark.range(0, 1000000).toDF("uid").withColumn("login", concat(lit('user_'), col('uid'))).\
        withColumn("email", concat(lit('user_'), col('uid'), lit('@gmail.com'))).\
        withColumn("user_state", lit('AZ'))
    usersDf.show(10)

    ordersDf = spark.range(0, 1000000).toDF("transaction_id").withColumn("quantity", col('transaction_id')).\
        withColumn("users_id", lit(random.randint(0, 1000000))). \
        withColumn("amount", lit(10 * col('transaction_id') * 0.2)). \
        withColumn("state", lit('AZ')).\
        withColumn("items", lit('SKU'))
    ordersDf.show(10)

    spark.sql("drop table if exists userstbl")

    # usersDf.sort(asc('uid')).\
    #     write.format('parquet').\
    #     bucketBy(8, 'uid').\
    #     mode('overwrite').\
    #     saveAsTable('userstbl')

    spark.sql("drop table if exists orderstbl")
    # ordersDf.sort(asc('users_id')).\
    #     write.format('parquet').\
    #     bucketBy(8, 'users_id').\
    #     mode('overwrite').\
    #     saveAsTable('orderstbl')

    # spark.sql("cache table userstbl")
    # spark.sql("cache table orderstbl")

    usersBucketDf = spark.table("userstbl")
    ordersBucketDf = spark.table("orderstbl")

    df = usersBucketDf.join(ordersBucketDf, usersBucketDf.uid == ordersBucketDf.users_id, 'inner')
    df.show(False)

    time.sleep(3600)