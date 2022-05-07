from pyspark.sql.types import *
class Airportx :
    def airport(self , spark ):
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
        if __name__ == 'Airport' :
            # raise Exception( """
            #  [1] P L E A S E  ---  P R O V I D E  ---  S C H E M A
            #                       or
            #  [2] P R O V I D E  ---  infer S C H E M A  = T R U E
            # """)
            print("""
            
                  *    A I R P O R T   ---   S H E E T     *
                  
************ HELP ************************************************************************ HELP ************************************************************************ HELP ************************************************************************ HELP🌝                      
AirportID ===*   Unique OpenFlights identifier for this airport.
Name	  ===*   Name of airport. May or may not contain the City name.
City	  ===*   Main city served by airport. May be spelled differently from Name.
Country	  ===*   Country or territory where airport is located. See Countries to cross-reference to ISO 3166-1 codes.
IATA	  ===*   3-letter IATA code.  (  Null if not assigned/unknown )
ICAO	  ===*   4-letter ICAO code.  (  Null if not assigned )  
Latitude  ===* 	 Decimal degrees, usually to six significant digits. Negative is South, positive is North.
Longitude ===*   Decimal degrees, usually to six significant digits. Negative is West, positive is East.
Altitude  ===*   Altitude number( In feet ).
Timezone  ===*   Hours offset from UTC. Fractional hours are expressed as decimals, eg. India is 5.5.
DST	      ===*   ( Daylight savings time ) One of E (Europe), A (US/Canada), S (South America), O (Australia), Z (New Zealand), N (None) or U (Unknown). See also: Help: Time
Tz        ===*   Database time zone	Timezone in "tz" (Olson) format, eg. "America/Los_Angeles".
Type	  ===*   Type of the airport. Value "airport" for air terminals, "station" for train stations, "port" for ferry terminals and "unknown" if not known. In airports.csv, only type=airport is included.
Source	  ===*   Source of this data. "OurAirports" for data sourced from OurAirports, "Legacy" for old data not matched to OurAirports (mostly DAFIF), "User" for unverified user contributions. In airports.csv, only source=OurAirports is included.
************ HELP ************************************************************************ HELP ************************************************************************ HELP ************************************************************************ HELP

                  
                +----------------------------------------------+  
                |  *    A I R P O R T   ---   T A B L E     *  |
                +----------------------------------------------+

            """)


        schema = StructType(
            [
                StructField( 'Airport_Id' ,IntegerType() , True ),
                StructField( 'Name' ,StringType(),True ),
                StructField('City', StringType(),True ),
                StructField('Country' , StringType(), True ),
                StructField('IATA', StringType(), True ),
                StructField('ICAO' ,StringType(),True ),
                StructField( 'Latitude' , FloatType() ,True ),
                StructField('Longitude' , FloatType() ,True ),
                StructField('Altitude' , IntegerType() ,True ),
                StructField('Timezone', StringType() ,True ),
                StructField( 'DST' ,StringType(), True ),
                StructField( 'TZ' ,StringType(), True ),
                StructField('Type' , StringType() ,True ),
                StructField('Source' ,StringType(), True  )
            ]
        )
        airport = spark.read\
        .csv("/users/radeon/downloads/AirlineData/airport.csv" , schema = schema  )
        return airport