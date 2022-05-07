from pyspark.sql.types import *
class Routesx :
    def routes(self , spark ):
        """
        Returns new dataFrame if passes the csv File
        :param spark: SparkSession
        :param path: airline.csv file path located in downloads
        :param schema: Provided as 'schema'
        :param inferSchema: None
        :param header: True
        :param sep: None
        :return:
        """
        if __name__ == 'Routes' :
            # raise Exception( """
            #  [1] P L E A S E  ---  P R O V I D E  ---  S C H E M A
            #                       or
            #  [2] P R O V I D E  ---  infer S C H E M A  = T R U E
            # """)
            print("""
            
                  *    R O U T E S    ---    S H E E T     *
                  
************ HELP ************************************************************************ HELP ************************************************************************ HELP 🦊                      
Airline	        ===*   2-letter (IATA) or 3-letter (ICAO) code of the airline.
AirlineID       ===*   Unique OpenFlights identifier for airline (see Airline).
Src_airport	    ===*   3-letter (IATA) or 4-letter (ICAO) code of the source airport.
Src_airport_id  ===*   Unique OpenFlights identifier for source airport (see Airport)
Desc_airport    ===*   3-letter (IATA) or 4-letter (ICAO) code of the destination airport.
Dest_airport_id ===*   Unique OpenFlights identifier for destination airport (see Airport)
Codeshare	    ===*   "Y" if this flight is a codeshare (that is, not operated by Airline, but another carrier), empty otherwise.
Stops	        ===*   Number of stops on this flight ("0" for direct)
Equipment	    ===*   3-letter codes for plane type(s) generally used on this flight, separated by spaces

The data is UTF-8 encoded. The special value    [ \ N  ]  is used for "NULL" to indicate that no value is available, and is understood automatically by MySQL if imported.
************ HELP ************************************************************************ HELP ************************************************************************ HELP 🦊

                  
                +--------------------------------------------+  
                |  *    R O U T E S   ---   T A B L E     *  |
                +--------------------------------------------+

            """)

        routes = spark.read\
            .parquet("/users/radeon/downloads/AirlineData/routes.snappy.parquet",header = True )
        return routes



