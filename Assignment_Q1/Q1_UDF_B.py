from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from Airport import *
from pyspark.sql.functions import when


if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Airline").master("local[*]") \
        .config("spark.driver.binAddress" ,"localhost") \
        .config("spark.ui.port","4059") \
        .getOrCreate()
    data = Airportx()
    airport = data.airport( spark )
    # airport.show()
    # airport.printSchema()

    cols = airport.columns
    L =  airport.dtypes
    schema = [ L[i][1] for i in range( len(L) ) ]


    def fun(elem):
        if elem == '\\N' or elem == 'null':
            return "(unknown)"
        else :
            return elem
    f = udf( fun , StringType() )


    for i,j in enumerate(cols) :
        if schema[i] == 'int':
            # x = airport
            airport = airport.withColumn( j , when( airport[j].isNull() , -1  ) \
                                          .otherwise( airport[ j ]  ).alias( j )  )

        if schema[i] == 'string':
            # y = airport
            airport = airport.withColumn( j , when( airport[j].isNull() , '(Unknown)'  ) \
                                          .otherwise( airport[ j ]  ).alias( j )  )
            """
            since,  fillna / na.fill
            are not designed to filter \ N 
            so passing through UDF  
            """
            airport = airport.withColumn( j , f(j))



    airport.show( 20000, truncate = False )
    print("Complete••••" )
    print(schema)
    airport.printSchema()

    """
    Writing file so that all unfetachable null values got removed 
    """
    try :
        airport.write.format('csv') \
            .save('/users/radeon/AirlineData/Airportx.csv')
        print("Saved")
    except Exception as E :
        print("Already_Saved !",E)





    # print("#################### M E T H O D  _  2 #######################")


