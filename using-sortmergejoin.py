import random
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, exp

if __name__ == '__main__':
    spark = SparkSession.builder.appName('using-sortmergejoin').getOrCreate()
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

    df = usersDf.join(ordersDf, usersDf.uid == ordersDf.users_id, 'inner')
    df.show(10, False)
    time.sleep(3600)