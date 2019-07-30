

'''
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit readfile_CP.py
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

def trimDollar(astring):
    return astring.strip("$")

def main(spark ):

    dobf = 'hdfs:/user/cp2530/DOB_raw'
    df = spark.read.csv(dobf, header = True, inferSchema = True)
    trim = F.udf(trimDollar)

    df = df.select(F.col("Job #").alias("job_num").cast('String'), F.col("Doc #").alias("doc_num"), 
         F.col('GIS_LATITUDE').alias('latitude'), F.col('GIS_LONGITUDE').alias('longitude'),  
         F.col('TOTAL_CONSTRUCTION_FLOOR_AREA').alias('construction_area').cast('Double'), 
         F.col('Job Description').alias('job_descrip'), 
         trim(F.col('Initial cost')).alias('initial_cost').cast('Double'),
         F.col('Job Type').alias('job_type'), F.to_date(F.col('Fully Permitted'), "MM/dd/yyyy").alias('permitted_date'), 
         F.col('Proposed Occupancy').alias('proposed_occup_code').cast('String'),
         F.col('GIS_NTA_NAME').alias('neighborhood'))
    
    print('data frame count: {}'.format(df.count()))
    print("schema")
    print(df.printSchema())
    
    df2 = df.withColumn('job_num_len', F.length('job_num')).withColumn('job_descrip_len', F.length('job_descrip'))
    print(df2.show(2))
    
    print('check max and min values')
    dftemp = df2.agg(F.min(F.col('doc_num')), F.max(F.col('doc_num')), 
                     F.min(F.col('latitude')), F.max(F.col('longitude')),
                     F.min(F.col('job_descrip_len')), F.max(F.col('job_descrip_len')),
                     F.min(F.col('initial_cost')), F.max(F.col('initial_cost')),
                     F.min(F.col('permitted_date')), F.max(F.col('permitted_date')))

    print(dftemp.show())

    print('check the distinct types of Field job_type, proposed_occup_code, and neighborhood ')

    print(df.groupby("job_type").agg(F.count('*')).show())
    print(df.groupby("proposed_occup_code").agg(F.count('*')).show())
    print(df.groupby("neighborhood").agg(F.count('*')).show())

    df.write.parquet('hdfs:/user/cp2530/DOBraw.parquet', mode="overwrite") 
    print('finished saving initial parquet') 



if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('project').getOrCreate()

    # Call our main routine
    main(spark)

