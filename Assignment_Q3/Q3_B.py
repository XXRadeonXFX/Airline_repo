#3 B. get airport details which is having minimum number of
# takeoff and landing.

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Q3_Assignment').master("local[*]") \
        .config("spark.driver.binAddress","localhost") \
        .config("spark.ui.port","4059") \
        .getOrCreate()
    routes = spark.read \
        .parquet("/users/radeon/downloads/AirlineData/routes.snappy*")
    routes.show()
    routes.groupBy('src_airport_id','airline','airline_id','src_airport') \
        .count().show(10000)