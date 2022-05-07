#3 A. get the airlines details like name, id,.
# which is has taken takeoff more than 3 times from same airport

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Q3_Assignment').master("local[*]")\
        .config("spark.driver.binAddress","localhost")\
        .config("spark.ui.port","4059")\
        .getOrCreate()
    routes = spark.read\
        .parquet("/users/radeon/downloads/AirlineData/routes.snappy*")
    routes.show()
    routes.groupBy('src_airport_id','airline','airline_id','src_airport')\
        .count().where('count > 3').show(10000)