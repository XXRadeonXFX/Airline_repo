from pyspark.sql import SparkSession
from readdatautil  import ReadDataUtil

if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Airline").master("local[*]")\
        .getOrCreate()
    rdu = ReadDataUtil()
    df = rdu.readCsv( spark = spark , path = "/users/radeon/downloads/employeex.csv" ,
                      inferSchema = True  )
    df.show()
    df.printSchema()

    airport = spark.read \
        .csv("/users/radeon/downloads/AirlineData/airport.csv")
    airport.show()
