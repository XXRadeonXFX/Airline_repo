from pyspark.sql  import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]")\
            .appName("Ax - Streaming")\
            .config("spark.driver.binAddress","localhost")\
            .config("spark.ui.port","4056")\
            .getOrCreate()
    sc = spark.sparkContext
    print(sc)
    df = spark.readStream.format("socket")\
        .option("host","localhost")\
        .option("port","9999")\
       .load()
    dfdisplay = df.writeStream.format("console")\
        .start()
    # dfdisplay.awaitTermination()
