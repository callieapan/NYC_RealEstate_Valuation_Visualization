import os
import re
import sys
import datetime as dt
from pyspark.sql import SparkSession


def generate_metrics(spark, input_file):
  df = spark.read.parquet(input_file)
  df.createOrReplaceTempView('three_one_one')
  # headers are year, incident_zip, complaint_type, incident_count
  incidents_per_zip = spark.sql("""
    WITH grouped AS (
      SELECT year, incident_zip, complaint_type, COUNT(complaint_type) AS incident_count
      FROM three_one_one
      GROUP BY 1, 2, 3)
    SELECT *
    FROM (SELECT ROW_NUMBER() OVER (PARTITION BY incident_zip ORDER BY incident_count DESC) AS rnum, 
          grouped.* FROM grouped) AS grouped_with_rnum
    WHERE rnum <= 5 
    ORDER BY incident_zip, year
    """)
  incidents_per_zip = incidents_per_zip.drop(*['rnum'])
  # headers are year, incident_zip, avg_job_time
  average_completion_time = spark.sql("""
    SELECT year, incident_zip, AVG(job_time) avg_job_time
    FROM three_one_one
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
    """)
  #headers are year, incident_zip, complaint_type, avg_job_time
  average_completion_time_by_incident = spark.sql("""
    WITH grouped AS (SELECT year, incident_zip, complaint_type, AVG(job_time) avg_job_time
            FROM three_one_one
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3 DESC)
    SELECT * 
    FROM (SELECT ROW_NUMBER() OVER (PARTITION BY incident_zip ORDER BY avg_job_time DESC) as rnum,
          grouped.* FROM grouped) as grouped_with_rnum
    WHERE rnum <= 5
    order by incident_zip, year
    """)
  average_completion_time_by_incident = average_completion_time_by_incident.drop(*['rnum']) 

  incidents_per_zip.write.csv('incidents_per_zip')
  average_completion_time.write.csv('average_completion_time')
  average_completion_time_by_incident.write.csv('average_completion_time_by_incident')


if __name__ == "__main__":
  spark = SparkSession.builder.appName("generate_metrics").getOrCreate()
  input_parquet = sys.argv[1]
  generate_metrics(spark, input_parquet)
