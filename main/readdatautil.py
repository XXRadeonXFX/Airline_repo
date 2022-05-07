class ReadDataUtil :
    def readCsv(self, spark, path,schema =None ,inferSchema = True, header = True ,sep = ","  ):
        """
        Returns new dataFrame if passes the csv File
        :param spark: Spark Session
        :param path:  CSV file path or directory
        :param schema: provide schema, required when inferSchema is false
        :param inferSchema:  if true : detect the schema else false : ignore autoDetected Schema
        :param header: if true : input csv file has header
        :param sep: default: "," specify seperator present in csv file
        :return:
        """
        if ( inferSchema ) == False and ( schema == None ):
            raise Exception("Please provide inferSchema as True else provide Schema for given input file")
        readdf = spark.read.csv( path = path , inferSchema = inferSchema , header = header , sep = sep)
        return readdf