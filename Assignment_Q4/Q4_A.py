#4 get airport details which is having
# maximum number of takeoff and landing.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from Airlinex import Airlinex
from Airportx import *
from Planex import *
from Routesx import *




if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Q5_Using_SQL") \
        .master("local[*]") \
        .config("spark.driver.binAddress", "localhost") \
        .config("spark.ui.port" ,"4059") \
        .getOrCreate()

    """
    AIRPLANE ############################################################
    """
    data = Airlinex()
    airline = data.airline( spark )
    airline.show( 40,truncate = False )
    airline.printSchema()
    """
    AIRPLANE ############################################################
    """


    """
    AIRPORT ############################################################
    """
    data = Airportx()
    airport = data.airport( spark )
    airport.show( 40,truncate = False )
    airport.printSchema()
    """
    AIRPORT ############################################################
    """


    """
    ROUTES ############################################################
    """
    data = Routesx()
    routes = data.routes( spark )
    routes.show( truncate = False )
    routes.printSchema()
    """
    ROUTES ############################################################
    """
    routes

