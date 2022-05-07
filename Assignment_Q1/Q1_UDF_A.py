from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from Airline import *
from pyspark.sql.functions import when


if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Airline").master("local[*]") \
        .config("spark.driver.binAddress" ,"localhost")\
        .config("spark.ui.port","4059")\
        .getOrCreate()
    data = Airlinex()
    airline = data.airline( spark )
    # airline.show()
    # airline.printSchema()

    cols = airline.columns
    L =  airline.dtypes
    schema = [ L[i][1] for i in range( len(L) ) ]


    def fun(elem):
        if elem == '\\N' or elem == 'null':
            return "(unknown)"
        else :
            return elem
    f = udf( fun , StringType() )


    for i,j in enumerate(cols) :
        if schema[i] == 'int':
            # x = airline
            airline = airline.withColumn( j , when( airline[j].isNull() , -1  ) \
               .otherwise( airline[ j ]  ).alias( j )  )

        if schema[i] == 'string':
            # y = airline
            airline = airline.withColumn( j , when( airline[j].isNull() , '(Unknown)'  ) \
                                          .otherwise( airline[ j ]  ).alias( j )  )
            """
            since,  fillna / na.fill
            are not designed to filter \ N 
            so passing through UDF  
            """
            airline = airline.withColumn( j , f(j))


    airline.show( 200, truncate = False )
    print("Complete••••" )
    print(schema)
    airline.printSchema()


    """
    Writing file so that all unfetachable null values got removed 
    """
    try :
        airline.write.format('csv')\
            .save('/users/radeon/AirlineData/Airlinex.csv')
        print("Saved")
    except Exception as E :
        print("Already_Saved !",E)


    # print("#################### M E T H O D  _  2 #######################")


