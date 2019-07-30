import os
import re
import sys
import datetime as dt
from pyspark.sql import SparkSession


def generate_metrics(spark, input_file):
  df = spark.read.parquet(input_file)
  df.createOrReplaceTempView('three_one_one')
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
  average_completion_time = spark.sql("""
    SELECT year, incident_zip, AVG(job_time)
    FROM three_one_one
    GROUP BY 1, 2
    """)
  average_completion_time_by_incident = spark.sql("""
    SELECT year, incident_zip, complaint_type, AVG(job_time)
    FROM three_one_one
    GROUP BY 1, 2, 3
    """)

  incidents_per_zip.write.csv('incidents_per_zip', header = True)
  average_completion_time.write.csv('average_completion_time', header = True)
  average_completion_time_by_incident.write.csv('average_completion_time_by_incident', header = True)


if __name__ == "__main__":
  spark = SparkSession.builder.appName("generate_metrics").getOrCreate()
  input_parquet = sys.argv[1]
  generate_metrics(spark, input_parquet)
