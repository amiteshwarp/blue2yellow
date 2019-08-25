from pyspark.sql import SparkSession,Window

from pyspark.sql.functions import lag,monotonically_increasing_id,when,col,desc,sum,last

spark = SparkSession.builder.appName('Basics').getOrCreate()

deliveris_df = spark.read.csv('data-sets/ipl/deliveries.csv', inferSchema=True, header=True)
matches_df = spark.read.csv('data-sets/ipl/matches.csv', inferSchema=True, header=True)

batsman_runs_df = deliveris_df.groupBy('batsman').agg( sum('batsman_runs').alias('total_runs') )
most_scored_df = batsman_runs_df.orderBy('total_runs', ascending = False )
least_scored_df = batsman_runs_df.filter(col('total_runs') > 0 ) .orderBy('total_runs', ascending= True )
most_scored_df.show(1)
least_scored_df.show(1)