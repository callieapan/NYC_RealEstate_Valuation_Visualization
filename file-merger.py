import os
import sys
import datetime as dt
from pyspark.sql import SparkSession
'''
spark-submit file-merger.py hdfs:/user/ctd299/valuation_growth_byzip.csv hdfs:/user/ctd299/valuation_yearly_growth_byzip.csv\
                            hdfs:/user/amr1059/dfDOBall.csv hdfs:/user/amr1059/avg_comp_time_transpose.csv
'''

def merge_files(spark, valuation_growth, valuation_growth_yearly, job_type_by_zip, service_requests_311):
  valuation = spark.read.csv(valuation_growth, header = True)
  valuation_yearly = spark.read.csv(valuation_growth_yearly, header = True)
  job_type = spark.read.csv(job_type_by_zip, header = True)
  service_requests = spark.read.csv(service_requests_311, header = True)

  valuation_yearly = valuation_yearly.withColumnRenamed('postcode', 'zipcode')
  columnsToDrop = ['boro', 'boro_name']
  valuation_yearly = valuation_yearly.drop(*columnsToDrop)

  valuation.createOrReplaceTempView('valuation')
  valuation_yearly.createOrReplaceTempView('val_yearly')
  job_type.createOrReplaceTempView('jobs')
  service_requests.createOrReplaceTempView('service')

  merged_files = spark.sql('''
      SELECT val.*, vy.*, jobs.*, svc.*
      FROM valuation val 
      LEFT JOIN val_yearly vy on vy.zipcode = val.postcode
      LEFT JOIN jobs on jobs.zip1 = val.postcode
      LEFT JOIN service svc on svc.incident_zip = val.postcode
    ''')
  columnsToDrop = ['incident_zip', 'zipcode', 'zip1']
  merged_files = merged_files.drop(*columnsToDrop)
  merged_file = merged_files.coalesce(1)
  merged_file.write.csv('merged_file', header = True)


if __name__ == "__main__":
  spark = SparkSession.builder.appName("merge_files").getOrCreate()
  valuation_file = sys.argv[1]
  valuation_yearly_file = sys.argv[2]
  jobs_file = sys.argv[3]
  service_requests = sys.argv[4]
  merge_files(spark, valuation_file, valuation_yearly_file, jobs_file, service_requests)

