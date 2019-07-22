 
'''
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit cleanfile_CP.py
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
 
 
 
def convertKeyWords(astring):
    if astring is None:
        return ""
    
    dicKeys = {}
    dicKeys["MODIFICATION"] = "modify"
    dicKeys["MODIFY"] = "modify"
    dicKeys["INSTALL"] = 'install'
    dicKeys["RENVOATION"] = 'renovate'
    dicKeys["RENVOATE"] = 'renovate'
    dicKeys["RENVOATING"] = 'renovate'
    dicKeys["CONVERSION"] = "convert"
    dicKeys["CONVERT"] = "convert"
    dicKeys["RESTORATION"] = "restore"
    dicKeys["RESTORE"] = "restore"
    dicKeys["RESTORING"] = "restore"
    dicKeys["REMOVE"] = "remove"
    dicKeys["REMOVAL"] = "remove"
    dicKeys["REMOVING"] = "remove"
    dicKeys["NEW"] = 'new'
    dicKeys["DEMOLITION"] = 'demolition'
    dicKeys["NO CHANGE"] ='no_change'

    alist = []
    for word in dicKeys.keys():
        if word in astring:
            alist.append(dicKeys[word])

    result = ",".join(alist)
    return result

def main(spark):
    df = spark.read.parquet("hdfs:/user/cp2530/DOBraw.parquet")

    #remove dates before 2009-12-31 and date after 2019-07-31
    df = df.filter(df.permitted_date<datetime.date(2017,7,31)).filter(df.permitted_date>datetime.date(2009,12,31))

    #save the coordinate pairs to csv for zipcode mapping
    df2coord = df.groupby('longitude', 'latitude').agg(F.count("*"))
    df2coord.write.csv('hdfs:/user/cp2530/DOBdf2coord', mode='overwrite')
    
    findKeyWords = F.udf(convertKeyWords)
    df = df.withColumn('job_descrip_keyword', findKeyWords(F.col('job_descrip')))

    print(df.show(2))
    print('min and max length of ob_descrip_keyword')
    print(df.agg(F.max(F.length('job_descrip_keyword'))).collect())
    print(df.agg(F.min(F.length('job_descrip_keyword'))).collect()))

    df.write.parquet('hdfs:/user/cp2530/DOBclean.parquet', mode="overwrite") 
    print('finished saving clean parquet') 



if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('project').getOrCreate()

    # Call our main routine
    main(spark)