import os
import re
import sys
import datetime as dt
from pyspark.sql import SparkSession
'''
spark-submit 311_metrics_adjustment.py hdfs:/user/amr1059/average_completion_time.parquet \
            hdfs:/user/amr1059/average_completion_time_by_incident.parquet hdfs:/user/amr1059/incidents_per_zip.parquet

'''

def adjust_and_transpose(spark, parquet_file1, parquet_file2, parquet_file3):
  table = spark.read.parquet(parquet_file1)
  table.createOrReplaceTempView('average_completion_time')
  table_transpose = spark.sql("""
    SELECT incident_zip, 
           SUM(coalesce(CASE WHEN year = 2010 THEN avg_job_time END, 0)) AS 2010_job_time,
           SUM(coalesce(CASE WHEN year = 2011 THEN avg_job_time END, 0)) AS 2011_job_time,
           SUM(coalesce(CASE WHEN year = 2012 THEN avg_job_time END, 0)) AS 2012_job_time,
           SUM(coalesce(CASE WHEN year = 2013 THEN avg_job_time END, 0)) AS 2013_job_time,
           SUM(coalesce(CASE WHEN year = 2014 THEN avg_job_time END, 0)) AS 2014_job_time,
           SUM(coalesce(CASE WHEN year = 2015 THEN avg_job_time END, 0)) AS 2015_job_time, 
           SUM(coalesce(CASE WHEN year = 2016 THEN avg_job_time END, 0)) AS 2016_job_time,
           SUM(coalesce(CASE WHEN year = 2017 THEN avg_job_time END, 0)) AS 2017_job_time,
           SUM(coalesce(CASE WHEN year = 2018 THEN avg_job_time END, 0)) AS 2018_job_time,
           SUM(coalesce(CASE WHEN year = 2019 THEN avg_job_time END, 0)) AS 2019_job_time 
    FROM average_completion_time
    GROUP BY incident_zi
    """)

  table2 = spark.read.parquet(parquet_file2)
  table2.createOrReplaceTempView('average_completion_time_by_incident')
  table2_transpose = spark.sql("""
    SELECT incident_zip, complaint_type,
           SUM(coalesce(CASE WHEN year = 2010 THEN avg_job_time END, 0)) AS 2010_job_time,
           SUM(coalesce(CASE WHEN year = 2011 THEN avg_job_time END, 0)) AS 2011_job_time,
           SUM(coalesce(CASE WHEN year = 2012 THEN avg_job_time END, 0)) AS 2012_job_time,
           SUM(coalesce(CASE WHEN year = 2013 THEN avg_job_time END, 0)) AS 2013_job_time,
           SUM(coalesce(CASE WHEN year = 2014 THEN avg_job_time END, 0)) AS 2014_job_time,
           SUM(coalesce(CASE WHEN year = 2015 THEN avg_job_time END, 0)) AS 2015_job_time, 
           SUM(coalesce(CASE WHEN year = 2016 THEN avg_job_time END, 0)) AS 2016_job_time,
           SUM(coalesce(CASE WHEN year = 2017 THEN avg_job_time END, 0)) AS 2017_job_time,
           SUM(coalesce(CASE WHEN year = 2018 THEN avg_job_time END, 0)) AS 2018_job_time,
           SUM(coalesce(CASE WHEN year = 2019 THEN avg_job_time END, 0)) AS 2019_job_time 
    FROM average_completion_time_by_incident
    GROUP BY incident_zip, complaint_type
    """)

  table3 = spark.read.parquet(parquet_file3)
  table3.createOrReplaceTempView('incidents_per_zip')
  table3_transpose = spark.sql("""
    SELECT incident_zip, complaint_type, 
           SUM(coalesce(CASE WHEN year = 2010 THEN incident_count END, 0)) AS 2010_incident_count,
           SUM(coalesce(CASE WHEN year = 2011 THEN incident_count END, 0)) AS 2011_incident_count,
           SUM(coalesce(CASE WHEN year = 2012 THEN incident_count END, 0)) AS 2012_incident_count,
           SUM(coalesce(CASE WHEN year = 2013 THEN incident_count END, 0)) AS 2013_incident_count,
           SUM(coalesce(CASE WHEN year = 2014 THEN incident_count END, 0)) AS 2014_incident_count,
           SUM(coalesce(CASE WHEN year = 2015 THEN incident_count END, 0)) AS 2015_incident_count, 
           SUM(coalesce(CASE WHEN year = 2016 THEN incident_count END, 0)) AS 2016_incident_count,
           SUM(coalesce(CASE WHEN year = 2017 THEN incident_count END, 0)) AS 2017_incident_count,
           SUM(coalesce(CASE WHEN year = 2018 THEN incident_count END, 0)) AS 2018_incident_count,
           SUM(coalesce(CASE WHEN year = 2019 THEN incident_count END, 0)) AS 2019_incident_count 
    FROM incidents_per_zip
    GROUP BY incident_zip, complaint_type
    """)
  table_transpose.write.csv('avg_comp_time_transpose')  
  table2_transpose.write.csv('avg_comp_time_incident_transpose')
  table3_transpose.write.csv('incidents_by_zip_transpose')

if __name__ == "__main__":
  spark = SparkSession.builder.appName("transpose_tables").getOrCreate()
  input_parquet1 = sys.argv[1]
  input_parquet2 = sys.argv[2]
  input_parquet3 = sys.argv[3]
  adjust_and_transpose(spark, input_parquet1, input_parquet2, input_parquet3)


