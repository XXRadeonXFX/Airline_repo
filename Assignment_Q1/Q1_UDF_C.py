from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from Plane import *
from pyspark.sql.functions import when


if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Airline").master("local[*]") \
        .config("spark.driver.binAddress" ,"localhost") \
        .config("spark.ui.port","4059") \
        .getOrCreate()
    data = Planex()
    plane = data.plane( spark )
    # plane.show()
    # plane.printSchema()

    cols = plane.columns
    L =  plane.dtypes
    schema = [ L[i][1] for i in range( len(L) ) ]


    def fun(elem):
        if elem == '\\N' or elem == 'null':
            return "(unknown)"
        else :
            return elem
    f = udf( fun , StringType() )


    for i,j in enumerate(cols) :
        if schema[i] == 'int':
            # x = plane
            plane = plane.withColumn( j , when( plane[j].isNull() , -1  ) \
                                          .otherwise( plane[ j ]  ).alias( j )  )

        if schema[i] == 'string':
            # y = plane
            plane = plane.withColumn( j , when( plane[j].isNull() , '(Unknown)'  ) \
                                          .otherwise( plane[ j ]  ).alias( j )  )
            """
            since,  fillna / na.fill
            are not designed to filter \ N 
            so passing through UDF  
            """
            plane = plane.withColumn( j , f(j))



    plane.show( 20000, truncate = False )
    print("Complete••••" )
    print(schema)
    plane.printSchema()


    """
    Writing file so that all unfetachable null values got removed
    """
    # try :
    #     plane.write.format('csv') \
    #         .save('/users/radeon/AirlineData/Planex.csv')
    #     print("Saved")
    # except Exception as E :
    #     print("Already_Saved !",E)



    # print("#################### M E T H O D  _  2 #######################")


