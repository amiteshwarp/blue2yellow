from classes import CJdbcDataFrame, CSparkSession

objSparkSession = CSparkSession.CSparkSession('Load Application Amount', {"spark.driver.extraClassPath": "D:/postgresql-42.2.5.jar"})
strSql = '(select * from application_stages) as t'
objDataFrame = CJdbcDataFrame.CJdbcDataFrame(objSparkSession.getSession(), strSql)
df = objDataFrame.getDataFrame()
df.printSchema()
df.show()