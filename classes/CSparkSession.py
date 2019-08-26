from pyspark.sql import SparkSession
from pyspark import SparkConf

class CSparkSession(object):
    objSparkSession = None

    def __init__(self, strApplicationName, dicConfig ):
        objSparkConfig = SparkConf().setAppName( strApplicationName )
        for key,value in dicConfig.items():
            objSparkConfig.set(key, value)

        self.objSparkSession = SparkSession.builder.config(conf=objSparkConfig).getOrCreate()

    def getSession(self):
        return self.objSparkSession




