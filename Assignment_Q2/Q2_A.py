# find the country name which is having both airlines and airport
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Country_commmon")\
    .master("local[*]")\
    .config("spark.driver.binAddress" ,"localhost")\
    .config("spark.ui.port","4059")\
    .getOrCreate()

    schema = StructType(
        [
            StructField( 'AirlineId',IntegerType() ,True ),
            StructField('Name', StringType() ,True ),
            StructField( 'Alias' ,StringType(), True  ),
            StructField( 'IATA' ,StringType(),True  ),
            StructField('ICAO' ,StringType() ,True ),
            StructField('CallSign' ,StringType(), True  ),
            StructField('Country_A' ,StringType(),True ),
            StructField('Availability',StringType(), True )
        ]
    )

    df1 = spark.read\
        .csv("/users/radeon/downloads/AirlineData/airline.csv" ,
             schema = schema  , header = True )
    # df1.show( truncate = False )

    schema = StructType(
        [
            StructField( 'Airport_Id' ,IntegerType() , True ),
            StructField( 'Name' ,StringType(),True ),
            StructField('City', StringType(),True ),
            StructField('Country_B' , StringType(), True ),
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


    df2 = spark.read \
        .csv("/users/radeon/downloads/AirlineData/airport.csv" ,
             schema = schema  )
    # df2.show( truncate = False)

    df_new= df1.join( df2 ,df1['Country_A'] == df2['Country_B'] ,'inner')
    df_new.withColumn('Country' , df_new['Country_A'] ).select('Country')\
        .distinct().show( 200, truncate = True )

