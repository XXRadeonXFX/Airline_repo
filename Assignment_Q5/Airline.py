from pyspark.sql.types import *
class Airlinex :
    def airline(self , spark ):
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
        if __name__ == 'Airline' :
            # raise Exception( """
            #  [1] P L E A S E  ---  P R O V I D E  ---  S C H E M A
            #                       or
            #  [2] P R O V I D E  ---  infer S C H E M A  = T R U E
            # """)
            print("""
            
                  *    A I R L I N E S   ---   S H E E T     *
                  
************ HELP ************************************************************************ HELP ************************************************************************ HELP ************************************************************************ HELP ****************************                       
AirlineID ===*	 Unique OpenFlights identifier for this airline.
Name      ===*	 Name of the airline.
Alias	  ===*   Alias of the airline. For example, All Nippon Airways is commonly known as "ANA".
IATA	  ===*   2-letter IATA code, if available.
ICAO	  ===*   3-letter ICAO code, if available.
Callsign  ===*	 Airline callsign.
Country	  ===*   Country or territory where airport is located. See Countries to cross-reference to ISO 3166-1 codes.
Active	  ===*   "Y" if the airline is or has until recently been operational, "N" if it is defunct. This field is not reliable: in particular, major airlines that stopped flying long ago, but have not had their IATA code reassigned (eg. Ansett/AN), will incorrectly show as "Y".
************ HELP ************************************************************************ HELP ************************************************************************ HELP ************************************************************************ HELP ****************************
                   
                +------------------------------------------------+  
                |  *    A I R L I N E S   ---   T A B L E     *  |
                +------------------------------------------------+

            """)


        schema = StructType(
            [
                StructField( 'AirlineId',IntegerType() ,True ),
                StructField('Name', StringType() ,True ),
                StructField( 'Alias' ,IntegerType(), True  ),
                StructField( 'IATA' ,StringType(),True  ),
                StructField('ICAO' ,StringType() ,True ),
                StructField('CallSign' ,StringType(), True  ),
                StructField('Country' ,StringType(),True ),
                StructField('Availability',StringType(), True )
            ]
        )
        airline = spark.read\
          .csv( "/users/radeon/downloads/AirlineData/airline.csv",
        schema = schema  ,header = True )

        return airline