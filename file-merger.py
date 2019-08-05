import os
import sys
import datetime as dt
from pyspark.sql import SparkSession
'''
spark-submit file-merger.py hdfs:/user/ctd299/valuation_growth_byzip.csv hdfs:/user/amr1059/dfDOBall.csv \
             hdfs:/user/amr1059/avg_comp_time_transpose.csv
'''

def merge_files(spark, valuation_growth, job_type_by_zip, service_requests_311):
  valuation = spark.read.csv(valuation_growth, header = True)
  job_type = spark.read.csv(job_type_by_zip, header = True)
  service_requests = spark.read.csv(service_requests_311, header = True)

  valuation.createOrReplaceTempView('valuation')
  job_type.createOrReplaceTempView('jobs')
  service_requests.createOrReplaceTempView('service')

  merged_files = spark.sql('''
      SELECT val.*, jobs.*, svc.*
      FROM valuation val 
      LEFT JOIN jobs on jobs.zip1 = val.postcode
      LEFT JOIN service svc on svc.incident_zip = val.postcode
    ''')
  columnsToDrop = ['incident_zip', 'zip1']
  merged_files = merged_files.drop(*columnsToDrop)
  merged_file = merged_files.coalesce(1)
  merged_file.write.csv('merged_file', header = True)


if __name__ == "__main__":
  spark = SparkSession.builder.appName("merge_files").getOrCreate()
  valuation_file = sys.argv[1]
  jobs_file = sys.argv[2]
  service_requests = sys.argv[3]
  merge_files(spark, valuation_file, jobs_file, service_requests)

