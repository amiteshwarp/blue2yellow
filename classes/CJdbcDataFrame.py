import configparser

class CJdbcDataFrame(object):
    objDataFrame = None

    def __init__(self, objSparkSession, strSQL ):
        objConfigParser = configparser.ConfigParser()
        objConfigParser.read("config/db_config.ini")
        db_prop = objConfigParser['postgresql']
        db_url = db_prop['url']
        username = db_prop['username']
        password = db_prop['password']

        self.objDataFrame = objSparkSession.read.format('jdbc').options(url=db_url, dbtable=strSQL).option("user",username).option("password",password).load()

    def getDataFrame(self):
        return self.objDataFrame
