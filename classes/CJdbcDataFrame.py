import configparser

class CJdbcDataFrame(object):
    objSparkSession = None
    objDataFrame = None
    source_db_config = []
    dest_db_config = []

    def __init__(self, objSparkSession ):
        self.objSparkSession = objSparkSession

    def loadReadConfig(self):
        objConfigParser = configparser.ConfigParser()
        objConfigParser.read("config/db_config.ini")
        return objConfigParser['SourceDb']

    def loadWriteConfig(self):
        objConfigParser = configparser.ConfigParser()
        objConfigParser.read("config/db_config.ini")
        return objConfigParser['DestinationDb']

    def loadDataFrame( self, strSql ):
        db_prop = self.loadReadConfig()
        return self.objSparkSession.read.format('jdbc').options(url=db_prop['url'], dbtable=strSql).option("user",db_prop['username']).option(
            "password", db_prop['password']).load()

    def saveDataFrame(self, objDataFrame, strDbtable ):
        db_prop = self.loadWriteConfig()
        objDataFrame.write.format('jdbc').options(url=db_prop['url'], dbtable=strDbtable).option("user",db_prop['username']).option(
            "password", db_prop['password']).save()


