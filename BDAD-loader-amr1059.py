'''
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
such that we run the following 
spark-submit BDAD-loader.py file:///scratch/amr1059/311_Service_Requests_from_2010_to_Present.csv load_01.parquet

'''

import os
import re
import sys
import datetime as dt
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

SECONDS_PER_DAY = 86400


def csv_to_parquet(spark, input_file, output_file):
  start_time = dt.datetime.now()
  print('{} | Reading file: {}'.format(start_time, os.path.basename(input_file)))
  csv = spark.read.csv(input_file, header = True, inferSchema = True)
  standardizedColNames = list(map(lambda col: col.lower().replace(' ', '_'), csv.columns))
  standardizedColNames = list(map(lambda col: re.sub(r'[()]', '', col), standardizedColNames))
  csv = csv.toDF(*standardizedColNames)
  print('{} | Finished reading file: {}'.format(dt.datetime.now(), dt.datetime.now() - start_time))

  print('{} | Converting date string to timestamp'.format(dt.datetime.now()))
  csv = csv.withColumn('job_time', (f.unix_timestamp('closed_date', 'MM/dd/yyyy hh:mm:ss aa') - \
                                    f.unix_timestamp('created_date', 'MM/dd/yyyy hh:mm:ss aa'))/ SECONDS_PER_DAY)

  csv = csv.withColumn('year', f.year(f.to_timestamp('created_date', 'MM/dd/yyyy hh:mm:ss aa')))
  print('Time elapsed: {}'.format(dt.datetime.now() - start_time))

  print('{} | Repartitioning dataframe: {}'.format(dt.datetime.now(), dt.datetime.now() - start_time))
  csv = csv.repartition(10000, 'Year')
  print('{} | Saving to parquet file: {}'.format(dt.datetime.now(), dt.datetime.now() - start_time))
  csv.write.parquet(output_file)
  print('{} | Finished job in {}'.format(dt.datetime.now(), dt.datetime.now() - start_time))




if __name__ == "__main__":
  spark = SparkSession.builder.appName("load_file_to_parquet").master("local").getOrCreate()
  input_csv = sys.argv[1]
  output_parquet = sys.argv[2]
  csv_to_parquet(spark, input_csv, output_parquet)
