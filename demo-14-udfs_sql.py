from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


def main():
    sparkSession = SparkSession \
        .builder \
        .appName('UDFS using SQL') \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField('Date', StringType(), False),
                         StructField('Open', DoubleType(), False),
                         StructField('High', DoubleType(), False),
                         StructField('Low', DoubleType(), False),
                         StructField('Close', DoubleType(), False),
                         StructField('Adjusted Close', DoubleType(), False),
                         StructField('Volume', IntegerType(), False),
                         StructField('Name', StringType(), True)
                         ])

    stockPricesDf = sparkSession \
        .readStream \
        .option('header', 'true') \
        .schema(schema) \
        .csv(
        'C:\\Users\\91974\\Downloads\\conceptualizing-processing-model-apache-spark-structured-streaming\\datasets\\stock_data')

    print(' ')
    print('Is the stream ready?')
    print(stockPricesDf.isStreaming)

    print(' ')
    print('Schema of the input stream')
    print(stockPricesDf.printSchema())

    def price_delta(price_open, price_close):
        return price_close - price_open

    sparkSession.udf.register("calculate_price_delta_udf", price_delta, DoubleType())

    stockPricesDf.createOrReplaceTempView('stock_prices')

    priceDeltaDf = sparkSession.sql(
        """SELECT Date, Name, Open, Close, 
                  calculate_price_delta_udf(Open, Close) as price_delta
           FROM stock_prices""")

    query = priceDeltaDf \
        .writeStream.outputMode('append') \
        .format('console') \
        .option('truncate', 'false') \
        .option('numRows', 30) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    main()