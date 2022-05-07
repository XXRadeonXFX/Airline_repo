from pyspark.sql.types import *
class Planex :
    def plane(self , spark ):
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
        if __name__ == 'Plane' :
            # raise Exception( """
            #  [1] P L E A S E  ---  P R O V I D E  ---  S C H E M A
            #                       or
            #  [2] P R O V I D E  ---  infer S C H E M A  = T R U E
            # """)
            print("""
            
                  *    P L A N E   ---   S H E E T     *
                  
************ HELP ************************************************************                      
Name	  ===*   Full name of the aircraft.
IATA code ===*	 Unique three-letter IATA identifier for the aircraft.
ICAO code ===*	 Unique four-letter ICAO identifier for the aircraft.
************ HELP ************************************************************

                  
                +------------------------------------------+  
                |  *    P L A N E   ---   T A B L E     *  |
                +------------------------------------------+

            """)


        schema = StructType(
            [
                StructField( 'Name' ,StringType() , True ),
                StructField( 'IATA_CODE' ,StringType(),True ),
                StructField('ICAO_CODE', StringType(),True ),
            ]
        )
        plane = spark.read\
        .csv("/users/radeon/downloads/AirlineData/plane.csv",header = True , schema = schema
             ,sep = "" )
        return plane