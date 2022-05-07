# 5. Get the airline details, which is having direct flights.
# details like :
#                airline id,
#                name,
#                source airport name,
#                destination airport name


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from Airlinex import Airlinex
from Airportx import *
from Planex import *
from Routesx import *




if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Q5_Using_SQL")\
    .master("local[*]")\
    .config("spark.driver.binAddress", "localhost")\
    .config("spark.ui.port" ,"4059")\
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


    routes.groupBy().count().show()
    routes.select('*')\
        .where('stops = 0').groupBy().count().show()

    routes_copy = routes.select('*') \
        .where('stops = 0')


    # routes_copy.show()
    condition = [ routes_copy.airline_id  == airline.AirlineId ]

    avail = routes_copy.join( airline , on = condition ) \
        .select('airline_id' ,'Name' , 'src_airport' , 'dest_airport',
                'airline' , 'Availability') \
        .distinct()

    routes_copy.join( airline , on = condition ) \
        .select('airline_id' ,'Name' , 'src_airport' , 'dest_airport',
                'airline' , 'Availability') \
        .where(" Availability = 'Y' ") \
        .distinct() \
        .groupBy().count()\
        .show( truncate = False )



    airport_codec1 = airport.select(
        airport['Name'].alias('src_Airport'),
        airport['City'].alias( 'src_City'),
        airport.Country.alias('src_Country'),
        airport.IATA.alias('src_IATA'),
        airport.ICAO.alias('src_ICAO') ,
        airport.Timezone.alias('src_Timezone'),
        airport.DST.alias('src_DST' ) )

    airport_codec2 = airport.select(
        airport['Name'].alias('dest_Airport'),
        airport['City'].alias( 'dest_City'),
        airport.Country.alias('dest_Country'),
        airport.IATA.alias('dest_IATA'),
        airport.ICAO.alias('dest_ICAO') ,
        airport.Timezone.alias('dest_Timezone'),
        airport.DST.alias('dest_DST' ) )


    condition1 = [ avail.src_airport == airport_codec1.src_IATA ]
    condition2 = [ avail.dest_airport == airport_codec2.dest_IATA ]

    print("""
                  
                +--------------------------------------------+  
                |  *    O U T P U T   ---   T A B L E     *  |
                +--------------------------------------------+
              
    """)

    avail.join( airport_codec1 ,on = condition1 )\
        .join( airport_codec2 , on =  condition2 )\
        .where(" Availability = 'Y' ")\
        .show( truncate = False )


    avail.join( airport_codec1 ,on = condition1 ) \
        .join( airport_codec2 , on =  condition2 ) \
    .printSchema()

    # airport_codec.show()

