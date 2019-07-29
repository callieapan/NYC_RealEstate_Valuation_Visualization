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


def main(spark):

    df =  spark.read.parquet('hdfs:/user/cp2530/DOBcleanzip.parquet')
    #create year column
    df2 = df.withColumn("year", F.year(df["permitted_date"]))

    #calculate yoy change in number of jobs done by zipcode by job type
    df3 = df2.groupby("zip1", "job_type" , ).agg(F.countDistinct('job_num').alias('job count'))
    #df.groupBy("A", "B").pivot("C", Seq("small", "large")).sum("D")
    #Seq(2010,2011, 2012,2013,2014,2015,2016,2017,2018,2019)
    df3=df2.groupby("zip1", "job_type" ).pivot("year").agg(F.countDistinct('job_num').alias('job count'))
    df3 = df3.withColumn("delta 2011", F.col('2011') -  F.col('2010'))
             .withColumn("delta 2012", F.col('2012') -  F.col('2011'))\
             .withColumn("delta 2013", F.col('2013') -  F.col('2012'))\
             .withColumn("delta 2014", F.col('2014') -  F.col('2013'))\
             .withColumn("delta 2015", F.col('2015') -  F.col('2014'))\
             .withColumn("delta 2016", F.col('2016') -  F.col('2015'))\
             .withColumn("delta 2017", F.col('2017') -  F.col('2016'))\
             .withColumn("delta 2018", F.col('2018') -  F.col('2017'))\
             .withColumn("delta 2019", F.col('2019') -  F.col('2018'))

    df3.write.csv("jobcountbyzip_job_type.csv", mode="overwrite")
    print('finish writing jobcountbyzip_job_type.csv')

    #calculate yoy cost change by zip code by job type

    #