from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType, BooleanType
from pyspark.sql.functions import col


def first_steps():
    df.select("IncidentNumber", "AvailableDtTm", "CallType").where("CallType != 'Medical Incident'").show()
    df.select("CallType").filter(col("CallType").isNotNull()).distinct().show()


def second_steps():
    renamed_df = df.withColumnRenamed("Delay", "ResponseDelayInMins")
    renamed_df.select("CallType", "ResponseDelayInMins").where(col("ResponseDelayInMins") > 5).show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName("fire-dept-processing").getOrCreate()
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField('IncidentNumber', IntegerType(), True),
                              StructField('CallType', StringType(), True),
                              StructField('CallDate', StringType(), True),
                              StructField('WatchDate', StringType(), True),
                              StructField('CallFinalDisposition', StringType(), True),
                              StructField('AvailableDtTm', StringType(), True),
                              StructField('Address', StringType(), True),
                              StructField('City', StringType(), True),
                              StructField('Zipcode', IntegerType(), True),
                              StructField('Battalion', StringType(), True),
                              StructField('StationArea', StringType(), True),
                              StructField('Box', StringType(), True),
                              StructField('OriginalPriority', StringType(), True),
                              StructField('Priority', StringType(), True),
                              StructField('FinalPriority', IntegerType(), True),
                              StructField('ALSUnit', BooleanType(), True),
                              StructField('CallTypeGroup', StringType(), True),
                              StructField('NumAlarms', IntegerType(), True),
                              StructField('UnitType', StringType(), True),
                              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                              StructField('FirePreventionDistrict', StringType(), True),
                              StructField('SupervisorDistrict', StringType(), True),
                              StructField('Neighborhood', StringType(), True),
                              StructField('Location', StringType(), True),
                              StructField('RowID', StringType(), True),
                              StructField('Delay', FloatType(), True)])

    df = spark.read.schema(fire_schema).option("header", "True").csv("sf-fire-calls.csv")

    # first_steps()

    second_steps()



