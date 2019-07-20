


#module load python/gnu/3.6.5
#module load spark/2.4.0


 # We need sys to get the command line arguments
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import struct
from pyspark.sql.functions import col
from pyspark.sql import SQLContext #think this is outdated
from pyspark.sql.types import *




def main(spark ):

    dobf = 'hdfs:/user/cp2530/DOB_raw'
    df = spark.read.csv(dobf, header = True, inferSchema = True)
    print('initial schema')
    print(df.printSchema())
    df = df.select(col("Job #").alias("job_num").cast('String'), col("Doc #").alias("doc_num"), col('GIS_LATITUDE').alias('latitude'), col('GIS_LONGITUDE').alias('longitude'),  
         col('TOTAL_CONSTRUCTION_FLOOR_AREA').alias('constuction_area').cast('Double'), col('Job Description').substr(1,10).alias('Job_Descrip'), 
         col('Initial cost').alias('initial_cost').strip('$').cast('Double'),
         col('Total Est. Fee').alias('total_est_fee').strip('$').cast('Double'), 
         col('Job Type').alias('job_type'), col('Fully Permitted').alias('fully_permitted_date').cast('String'), col('Proposed Occupancy').alias('proposed_occup_code').cast('String'),
         col('GIS_NTA_NAME').alias('neighborhood'))
    
    print('after schema')
    print(df.printSchema())

    df.write.parquet('hdfs:/user/cp2530/DOBraw.parquet', mode="overwrite") 

    print('finished') 

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