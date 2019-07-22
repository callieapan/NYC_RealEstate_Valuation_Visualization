'''
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit mergzip_CP.py
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

    df = spark.read.parquet('hdfs:/user/cp2530/DOBclean.parquet')

    schema = StructType([ StructField('longitude', DoubleType()), StructField('latitude', DoubleType()), StructField('count', IntegerType()), 
                        StructField('zip1', StringType()), StructField('zip2', StringType())])
    zipdf = spark.read.csv('hdfs:/user/cp2530/dfcoordsmR.csv', header = True, schema = schema)
    print(zipdf.printSchema())


    zipdf = zipdf.select(F.col('longitude'),F.col('latitude'),F.col( 'zip1'),F.col( 'zip2'))

    #need to round everything to 6 decimals first
    df = df.withColumn("latitude", F.round(df["latitude"], 6)).withColumn('longitude', F.round(df["longitude"], 6))
    zipdf = zipdf.withColumn("latitude", F.round(zipdf["latitude"], 6)).withColumn('longitude', F.round(zipdf["longitude"], 6))
    #join the two
    df2 = df.join(zipdf, ["longitude", "latitude"], 'left')

    #check how many zip codes are nulls
    print(df2.filter(df2['zip1'].isNull()).count())
    print(df2.show(2))

    df.write.parquet('hdfs:/user/cp2530/DOBcleanzip.parquet', mode="overwrite") 
    print('finished saving cleanzip parquet') 

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('project').getOrCreate()

    # Call our main routine
    main(spark)