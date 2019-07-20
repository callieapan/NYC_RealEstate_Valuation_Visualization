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
NYC_ZIPCODES = [10178, 10270, 10271, 10278, 10279, 10280, 10281, 10282, 10301, 10302,
                10303, 10304, 10305, 10306, 10307, 10308, 10309, 10310, 10311, 10312, 
                10314, 11351, 11354, 11355, 11356, 11357, 11358, 11359, 11360, 11361, 
                11362, 11363, 11364, 11365, 11366, 11367, 11368, 11369, 11370, 11371,
                11372, 11373, 11374, 11375, 11377, 11378, 11379, 11385, 11411, 11412,
                11413, 11414, 11415, 11416, 11417, 11418, 11419, 11420, 11421, 11422,
                11423, 11426, 11427, 11428, 11429, 11430, 11432, 11433, 11434, 11435,
                11436, 10451, 10452, 10453, 10454, 10455, 10456, 10457, 10458, 10459,
                10460, 10461, 10462, 10463, 10464, 10465, 10466, 10467, 10468, 10469,
                10470, 10471, 10472, 10473, 10474, 10475, 11004, 10001, 10002, 10003,
                10004, 10005, 10006, 10007, 10009, 10010, 10011, 10012, 10013, 10014,
                10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10024,
                10025, 10026, 10027, 10028, 10029, 10030, 10031, 10032, 10033, 10034,
                10035, 10036, 10037, 10038, 10039, 10040, 10041, 10044, 10045, 10048,
                10055, 10060, 10069, 11101, 11102, 11103, 11104, 11105, 11106, 11109,
                10090, 10095, 10098, 10099, 10103, 10104, 10105, 10106, 10107, 10110,
                10111, 10112, 10115, 10118, 10119, 10120, 10121, 10122, 10123, 10128,
                10151, 10152, 10153, 10154, 10155, 11691, 11692, 10158, 11693, 11694,
                10161, 10162, 11697, 10165, 10166, 10167, 10168, 10169, 10170, 10171,
                10172, 10173, 10174, 10175, 10176, 11201, 10177, 11203, 11204, 11205,
                11206, 11207, 11208, 11209, 11210, 11211, 11212, 11213, 11214, 11215,
                11216, 11217, 11218, 11219, 11220, 11221, 11222, 11223, 11224, 11225,
                11226, 10199, 11228, 11229, 11230, 11231, 11232, 11233, 11234, 11235,
                11236, 11237, 11238, 11239, 11241, 11242, 11243, 11249, 11252, 11256]


def csv_to_parquet(spark, input_file, output_file):
  start_time = dt.datetime.now()
  print('{} | Reading file: {}'.format(start_time, os.path.basename(input_file)))
  csv = spark.read.csv(input_file, header = True, inferSchema = True)
  standardizedColNames = list(map(lambda col: col.lower().replace(' ', '_'), csv.columns))
  standardizedColNames = list(map(lambda col: re.sub(r'[()]', '', col), standardizedColNames))
  csv = csv.toDF(*standardizedColNames)
  print('{} | Finished reading file: {}'.format(dt.datetime.now(), dt.datetime.now() - start_time))

  print('{} | Converting date string to timestamp'.format(dt.datetime.now()))
  csv = csv.withColumn('created_date', f.to_timestamp('created_date', 'MM/dd/yyyy hh:mm:ss aa'))
  csv = csv.withColumn('closed_date', f.to_timestamp('closed_date', 'MM/dd/yyyy hh:mm:ss aa'))
  csv = csv.withColumn('job_time', (f.unix_timestamp('closed_date', 'MM/dd/yyyy hh:mm:ss aa') - \
                                    f.unix_timestamp('created_date', 'MM/dd/yyyy hh:mm:ss aa'))/ SECONDS_PER_DAY)
  csv = csv.withColumn('year', f.year('created_date'))

  print('{} | Standardizing city names'.format(dt.datetime.now()))
  csv = csv.withColumn('city', f.regexp_replace(f.lower(f.col('city')), r'\s',''))

  print('{} | Converting zipcodes to Integer'.format(dt.datetime.now()))
  csv = csv.withColumn('incident_zip', csv['incident_zip'].cast(t.IntegerType()))
  print('{} | Filtering out invalid zipcodes, retaining valid NYC zipcodes'.format(dt.datetime.now()))
  csv = csv.where(csv.incident_zip.isin(NYC_ZIPCODES))

  print('{} | Dropping columns'.format(dt.datetime.now()))
  columnsToDrop = ['location', 'x_coordinate_state_plane', 'y_coordinate_state_plane', 'resolution_action_updated_date', 'community_board',
                    'bbl', 'road_ramp']
  csv = csv.drop(*columnsToDrop)
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
