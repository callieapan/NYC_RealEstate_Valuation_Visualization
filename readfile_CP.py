

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
#from pyspark.sql.functions import isnan, when, count, col, to_date

from pyspark.sql import Row
import numpy as np
#from pyspark.sql.functions import collect_list
#from pyspark.sql.functions import struct
#from pyspark.sql.functions import col
from pyspark.sql import SQLContext #think this is outdated
from pyspark.sql.types import *
#from pyspark.sql.functions import udf
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


    #df2 = df.withColumn('job_num_len', F.length('job_num')).withColumn(....)
    #df2.registerTempTable("df2_table")
    #maxval["permitted_date"] = spark.sql("SELECT MAX(permitted_date) FROM df2_table").collect()[0]
    #minval["permitted_date"] = spark.sql("SELECT MIN (permitted_date) FROM df2_table").collect()[0]
    
    

    df.write.parquet('hdfs:/user/cp2530/DOBraw.parquet', mode="overwrite") 
    print('finished saving initial parquet') 


    #data clearning
    #remove rows where initial_cost is 999999999 or of a certain value

    #remove rows where permitted_date is > datetime.date(2019,7,31)
    df = df.filter()
    #create DateCol



if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('project').getOrCreate()

    # Call our main routine
    main(spark)






        # Schema = StructType([
    #         StructField("Job_no", StringType(), True),
    #         StructField("Doc_no", IntegerType(), True),
    #         StructField("Borough", StringType(), True),
    #         StructField("House_no", StringType(), True),
    #         StructField("Street_Name", StringType(), True),
    #         StructField("Block", IntegerType(), True),
    #         StructField("Lot", IntegerType(), True),
    #         StructField("Bin_no", IntegerType(), True),
    #         StructField("Job_Type", StringType(), True),
    #         StructField("Job_Status", StringType(), True),
    #         StructField("Job_Status_Desc", StringType(), True),
    #         StructField("Latest_Action_Date", StringType(), True), #need to convert this to datetime later
    #         StructField("Building_ Type", StringType(), True),
    #         StructField("Community-Board", IntergerType(), True),
    #         StructField("Cluster", StringType(), True),
    #         StructField("Landmarked", StringType(), True),
    #         StructField("Adult_Estab", StringType(), True),
    #         StructField("City_Owned", BooleanType(), True),
    #         StructField("Little_e", StringType(), True),
    #         StructField("PC_Filed", StringType(), True),
    #         StructField("eFilling_Filed", StringType(), True),
    #         StructField("Plumbing", StringType(), True),
    #         StructField("Mechanical", StringType(), True),
    #         StructField("Boiler", StringType(), True),
    #         StructField("Fuel_Burning", StringType(), True),
    #         StructField("Fuel_Storage", StringType(), True),
    #         StructField("Landmarked", StringType(), True),
    #         StructField("col6", DoubleType(), True)])
    
    #rdd = spark.textFile("hdfs:/user/cp2530/CSCI3033/DOB_raw.csv")
    #df = sqlContext.createDataFrame(rdd, schema)
    #df.write.parquet("hdfs:/user/cp2530/CSCI3033/DOB_raw.parquet")
    #col('Job Description').substr(1,100).alias('Job_Descrip')


    '''
    sample code


    df.registerTempTable("df_table")
    spark.sql("SELECT MAX(A) as maxval FROM df_table").collect()[0].asDict()['maxval']


    from pyspark.sql.functions import isnan, when, count, col

df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()
+-------+----------+---+
|session|timestamp1|id2|
+-------+----------+---+
|      0|         0|  3|
+-------+----------+---+




>>> dfdate = df.withColumn('permitted_date', to_date(col('fully_permitted_date'), "MM/dd/yyyy")


eval `ssh-agent -s`
ssh-add
    '''