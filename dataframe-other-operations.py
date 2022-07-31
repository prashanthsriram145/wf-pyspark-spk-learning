from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':
    spark = SparkSession.builder.appName('dataframe-other-operations').getOrCreate()

    tripdelaysFilePath = "departuredelays.csv"
    airportsnaFilePath = "airport-codes-na.txt"

    # Obtain airports data set
    airportsna = (spark.read
                  .format("csv")
                  .options(header="true", inferSchema="true", sep="\t")
                  .load(airportsnaFilePath))

    airportsna.createOrReplaceTempView("airports_na")

    # Obtain departure delays data set
    departureDelays = (spark.read
                       .format("csv")
                       .options(header="true")
                       .load(tripdelaysFilePath))

    departureDelays = (departureDelays
                       .withColumn("delay", expr("CAST(delay as INT) as delay"))
                       .withColumn("distance", expr("CAST(distance as INT) as distance")))

    departureDelays.createOrReplaceTempView("departureDelays")

    # Create temporary small table
    foo = (departureDelays
           .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
            date like '01010%' and delay > 0""")))
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    bar = departureDelays.union(foo)
    bar.createOrReplaceTempView("bar")

    # Show the union (filtering for SEA and SFO in a specific time range)
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()

    # Join departure delays data (foo) with airport info
    foo.join(
        airportsna,
        airportsna.IATA == foo.origin
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    spark.sql("SET -v").select("key", "value").show(n=5, truncate=False)
