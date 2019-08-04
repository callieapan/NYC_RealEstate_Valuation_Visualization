# Using Data to Drive Decisions related to New York Real Estate 

## Dataset #1
Real Estate Valuation data from Open Data NYC, data has been downloaded from the web and uploaded to HDFS.
Original dataset can be accessed at hdfs:/user/ctd299/Property_Valuation_and_Assessment_Data.csv

"Valuation_Cleaning.txt" contains commands to launch REPL, clean the data and save to a new file at hdfs:/user/ctd299/cleaned_data_2.csv
"Valuation Processing.txt" contains commands to launch REPL, calculate year over year valuation growth and categorizations, and save 2 files:
1) Zip code average yearly valuation growth over entire time range hdfs:/user/ctd299/valuation_growth_byzip.csv
2) Zip code average yearly valuation growth for each year in dataset hdfs:/user/ctd299/valuation_yearly_growth_byzip.csv

## Dataset #2
311 Service Requests 2010 to Present (sourced from Open Data NYC-- available at https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)




## Dataset #3
New Construction Permit Application Data from Open Data NYC
script files:
readfile_CP.py
cleanfile_CP.py
mergzip_CP.py
metricCalc_CP.py

additional csv files for input:
dfcoordsmR.csv

output file:
DOBsumcostbyzip_job_type.csv
dfDOBall.csv

i) The data has already been downloaded from https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD to hdfs directory "hdfs:/user/cp2530/DOB_raw" the file format is csv

ii) To read the data, run:
> module load python/gnu/3.6.5
> module load spark/2.4.0
> alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
> spark-submit readfile_CP.py

these commands will read in the file and save it into this directory 'hdfs:/user/cp2530/DOBraw.parquet'


iii) to clean the data, run:
> module load python/gnu/3.6.5
> module load spark/2.4.0
> alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
> spark-submit cleanfile_CP.py 

the clearnfile_CP.py script will read in the hdfs:/user/cp2530/DOBraw.parquet, truncate the data to the desired date range, create the job description keywords field


iv) to add the zipcode columns to the dataset, make sure dfcoordsmR.csv is in your current directory, run: 
> module load python/gnu/3.6.5
> module load spark/2.4.0
> alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
> spark-submit mergzip_CP.py

this step will write its output to this directory "hdfs:/user/cp2530/DOBcleanzip.parquet"

v) to calculate the metrics that will be used in the final visualization, run:
> module load python/gnu/3.6.5
> module load spark/2.4.0
> alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
> spark-submit metricCalc_CP.py
> hdfs dfs -getmerge hdfs:/user/cp2530/sumcostbyzip_job_type DOBsumcostbyzip_job_type.csv

the output files will be DOBsumcostbyzip_job_type.csv and dfDOBall.csv in your current directory 


Merging all Files
.....

