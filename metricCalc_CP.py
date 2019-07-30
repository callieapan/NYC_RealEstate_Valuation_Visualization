'''
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit metricCalc_CP.py
'''

 # We need sys to get the command line arguments
import sys
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row
import numpy as np
from pyspark.sql.types import *
import datetime
from functools import *


def makeaggs(df2):
    #calculate yoy change in number of jobs done by zipcode by job type
    df3 = df2.groupby("zip1", "job_type" , ).agg(F.countDistinct('job_num').alias('job count'))
    #df.groupBy("A", "B").pivot("C", Seq("small", "large")).sum("D")
    #Seq(2010,2011, 2012,2013,2014,2015,2016,2017,2018,2019)
    df3=df2.groupby("zip1", "job_type" ).pivot("year").agg(F.countDistinct('job_num').alias('job count'))
    print(df3.printSchema())
    
    df3 = df3.withColumn("delta 2011", F.col('2011') -  F.col('2010'))\
             .withColumn("delta 2012", F.col('2012') -  F.col('2011'))\
             .withColumn("delta 2013", F.col('2013') -  F.col('2012'))\
             .withColumn("delta 2014", F.col('2014') -  F.col('2013'))\
             .withColumn("delta 2015", F.col('2015') -  F.col('2014'))\
             .withColumn("delta 2016", F.col('2016') -  F.col('2015'))\
             .withColumn("delta 2017", F.col('2017') -  F.col('2016'))\
             .withColumn("delta 2018", F.col('2018') -  F.col('2017'))\
             .withColumn("delta 2019", F.col('2019') -  F.col('2018'))

    print('write file')
    df3.write.csv("hdfs:/user/cp2530/jobcountbyzip_job_type", mode="overwrite")
    print('finish writing jobcountbyzip_job_type in csv')

    #calculate yoy sum cost change by zip code by job type

    df4=df2.groupby("zip1", "job_type" ).pivot("year").agg(F.sum('initial_cost').alias('total cost'))
    print(df4.printSchema())
    
    df4 = df4.withColumn("delta 2011", F.col('2011') -  F.col('2010'))\
             .withColumn("delta 2012", F.col('2012') -  F.col('2011'))\
             .withColumn("delta 2013", F.col('2013') -  F.col('2012'))\
             .withColumn("delta 2014", F.col('2014') -  F.col('2013'))\
             .withColumn("delta 2015", F.col('2015') -  F.col('2014'))\
             .withColumn("delta 2016", F.col('2016') -  F.col('2015'))\
             .withColumn("delta 2017", F.col('2017') -  F.col('2016'))\
             .withColumn("delta 2018", F.col('2018') -  F.col('2017'))\
             .withColumn("delta 2019", F.col('2019') -  F.col('2018'))

    print(df4.show(3))
    print('write file')
    df4.write.csv("hdfs:/user/cp2530/sumcostbyzip_job_type", mode="overwrite")
    print('finish writing sumcostbyzip_job_type in csv')

    ##calculate yoy avg cost change by zip code by job type
    df5=df2.groupby("zip1", "job_type" ).pivot("year").agg(F.avg('initial_cost').alias('avg cost'))
    print(df5.printSchema())

    df5 = df5.withColumn("delta 2011", F.col('2011') -  F.col('2010'))\
             .withColumn("delta 2012", F.col('2012') -  F.col('2011'))\
             .withColumn("delta 2013", F.col('2013') -  F.col('2012'))\
             .withColumn("delta 2014", F.col('2014') -  F.col('2013'))\
             .withColumn("delta 2015", F.col('2015') -  F.col('2014'))\
             .withColumn("delta 2016", F.col('2016') -  F.col('2015'))\
             .withColumn("delta 2017", F.col('2017') -  F.col('2016'))\
             .withColumn("delta 2018", F.col('2018') -  F.col('2017'))\
             .withColumn("delta 2019", F.col('2019') -  F.col('2018'))

    df5.write.csv("hdfs:/user/cp2530/avgcostbyzip_job_type", mode="overwrite")
    print('finish writing avgcostbyzip_job_type in csv')

def main(spark):
    print('in main')
    df =  spark.read.parquet('hdfs:/user/cp2530/DOBcleanzip.parquet')
    #create year column
    df2 = df.withColumn("year", F.year(df["permitted_date"]))

    #makeaggs(df2)
    

    #calculate by year number of jobs with keyword 
    keywords = ['modify', 'install', 'renovate','convert', 'restore', 'new', 'remove', 'demolition', 'no_change']
    dfList = []
    for w in keywords:
        temp = df2.filter(df2["job_descrip_keyword"].contains(w))
        temp = temp.groupby("zip1").pivot("year").agg(F.countDistinct('job_num'))
        tempPanda = temp.toPandas()
        newcol = ['zip1']+[s+w+"_cnt" for s in tempPanda.columns[1:]]
        tempPanda.columns = newcol
        dfList.append(tempPanda)
        
    dfkeyword = reduce(lambda x, y: pd.merge(x, y, on = 'zip1'), dfList) 
    print(dfkeyword.columns)
    print(dfkeyword.head(2))

    dfkeyword.to_csv("dfkeyword.csv")
    print('finish write dfkeyword.csv')

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('project').getOrCreate()

    # Call our main routine
    main(spark)