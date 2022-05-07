from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from Routes import *
from pyspark.sql.functions import when


if __name__ == '__main__' :
    spark = SparkSession.builder.appName("Airline").master("local[*]") \
        .config("spark.driver.binAddress" ,"localhost") \
        .config("spark.ui.port","4059") \
        .getOrCreate()
    data = Routesx()
    routes = data.routes( spark )
    # routes.show()
    # routes.printSchema()

    cols = routes.columns
    L =  routes.dtypes
    schema = [ L[i][1] for i in range( len(L) ) ]


    def fun(elem):
        if elem == '\\N' or elem == 'null':
            return "(unknown)"
        else :
            return elem
    f = udf( fun , StringType() )


    for i,j in enumerate(cols) :
        if schema[i] == 'int':
            # x = routes
            routes = routes.withColumn( j , when( routes[j].isNull() , -1  ) \
                                      .otherwise( routes[ j ]  ).alias( j )  )

        if schema[i] == 'string':
            # y = routes
            routes = routes.withColumn( j , when( routes[j].isNull() , '(Unknown)'  ) \
                                      .otherwise( routes[ j ]  ).alias( j )  )
            """
            since,  fillna / na.fill
            are not designed to filter \ N 
            so passing through UDF  
            """
            routes = routes.withColumn( j , f(j))



    routes.show( 20000, truncate = False )
    print("Complete••••" )
    print(schema)
    routes.printSchema()

    """
    Writing file so that all unfetachable null values got removed 
    """
    try :
        routes.write.format('parquet') \
            .save('/users/radeon/AirlineData/Routesx.parquet')
        print("Saved")
    except Exception as E :
        print("Already_Saved !",E)



    # print("#################### M E T H O D  _  2 #######################")


