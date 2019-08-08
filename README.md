# Using Data to Drive Decisions related to New York Real Estate 
## Repo directory structure

    ├── README.md 
    ├── app_code
    │   └── 311_metrics_adjustment.py
    │   └── 311_metrics_amr1059.py
    │   └── metricCalc_CP.py
    ├── data_ingest
    │   └── 311_data_ingest_command.txt
    │   └── ingestcode.txt
    ├── etl_code
    │   └── 311-file-loader.py
    │   └── cleanfile_CP.py
    │   └── dfcoordsmR.csv
    │   └── mapCoordtoZip.ipynb
    │   └── mergzip_CP.py
    ├── profiling_code
    │   └── 311_profiling_commands.txt
    │   └── readfile_CP.py
    ├── screenshots
        └── 311_ETL.png
        └── 311_metrics_calculation.png
        └── 311_metrics_df_transpose.png
        └── file_merge.png
        └── Screenshoot _cleanfile_CP.docx
        └── Screenshoot _mergzip_CP.docx
        └── Screenshoot _metricCalc_CP.docx
        └── Screenshoot _readfile_CP.docx

## Repo directory & files explained
* /app_code
    * `311_metrics_amr1059.py`: python script that reads 311 parquet, runs queries for metrics using SparkSQL
    * `311_metrics_adjustment.py`: python script that transposes, aggregates, and reduces dimensions from above metrics through SparkSQL
* /data_ingest
    * `311_data_ingest_command.txt`: terminal command for ingesting 311 service request data to `/scratch/amr1059`
* /etl_code
    * `311-file-loader.py`: python script that read in 311 service data, converts strings to appropriate types, and drops unused columns and rows
* /profiling_code
    * `311_profiling_commands.txt`: contains various SparkSQL commands used for profiling data, i.e. seeing distribution of values and consistency of data
* /screenshots
    * `311_ETL.png`: displays ETL of 311 service data successfully running
    * `311_metrics_calculation.png`: displays initial 311 service metrics calculation through SparkSQL successfully running
    * `311_metrics_df_transpose.png`: displays transpose and dimension reduction of above 311 metrics successfully running
    * `file_merge.png`: displays successful merging of real estate valuation, DOB job, and 311 service requests data


## Dataset #1: Real Estate Valuation data
Real Estate Valuation data from Open Data NYC, data has been downloaded from the web and uploaded to HDFS.
Original dataset can be accessed at hdfs:/user/ctd299/Property_Valuation_and_Assessment_Data.csv

"Valuation_Cleaning.txt" contains commands to launch REPL, clean the data and save to a new file at hdfs:/user/ctd299/cleaned_data_2.csv
"Valuation Processing.txt" contains commands to launch REPL, calculate year over year valuation growth and categorizations, and save 2 files:
1) Zip code average yearly valuation growth over entire time range hdfs:/user/ctd299/valuation_growth_byzip.csv
2) Zip code average yearly valuation growth for each year in dataset hdfs:/user/ctd299/valuation_yearly_growth_byzip.csv

## Dataset #2: 311 Service Requests 2010 to Present
sourced from Open Data NYC. Available at https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
The following alias commands were used.

```
alias mods='module load python/gnu/3.6.5 && module load spark/2.4.0'
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
```
When starting in the terminal, prior to submiting any spark jobs it is important to first run the `mods` command; it is only necessary to run once. This is done to load python and spark. 
Corresponding python scripts:
```
file-loader-amr1059.py
311_metrics_amr1059.py
311_metrics_adjustment.py
```
### Ingesting data
First, load the data to HDFS scratch folder 
`scp -r ~/Downloads/311_Service_Requests_from_2010_to_Present.csv amr1059@dumbo.hpc.nyu.edu:/scratch/amr1059`

### Loading and pre-processing data
The following command loads the 311 file, converts columns to appropriate types, creates additional columns, and drops extraneous/unused columns. 
`spark-submit BDAD-loader.py file:///scratch/amr1059/311_Service_Requests_from_2010_to_Present.csv 311_service_requests.parquet`
For example, dates were originally parsed as strings. In order to accomplish any calculations date columns, `created_date`, such as needed to be converted to timestamps. Additionally, there was the creation of the `year` column, which is integer type and indicates the year the service request was placed. This was particularly crucial for any grouping and filtering. Lastly, `job_time` was a column created by subtracting `created_date` from `closed_date`. This computation alone returns number of seconds elapsed. To get the number of days elapsed between a service request being placed and when it is resolved, divide by the number of seconds in a day (86,400).

Output file is saved to `hdfs:/user/amr1059/`

### Generating metrics
The following command takes the output parquet file from the pre-processing phase and calculates several metrics through sql queries. These dataframes are then written to parquet files. 
`spark-submit 311_metrics_amr1059.py hdfs:/user/amr1059/311_service_requests.parquet`

- `incidents_per_zip.parquet` contains the five most occurring 311 service complaints for each zipcode, broken down by year
- `average_completion_time.parquet` contains the average completion time for a 311 service request for each zipcode, broken down by year
- `average_completion_time_by_incident` contains the average completion time for a specified 311 service request complaint, e.g. **_Indoor Sewage_** or **_Animal Abuse_**, for each zipcode, broken down by year

Output files are saved to `hdfs:/user/amr1059/`

### Transposing dataframes
For the purposes of merging our three distinct data sets, the 311 service data needs to be transposed such that each zipcode appears once per row. Every additional column corresponds to a specific year and the associated metric. For example, `incidents_per_zip` transposed contains a `2011_incident_count` column for the tally of incidents recorded in a zipcode for 2011. The following command transposes the output parquet files from the previous step.
`spark-submit 311_metrics_adjustment.py hdfs:/user/amr1059/average_completion_time.parquet hdfs:/user/amr1059/average_completion_time_by_incident.parquet hdfs:/user/amr1059/incidents_per_zip.parquet`

- `avg_comp_time_transpose` is the transpose of `average_completion_time.parquet`
- `avg_comp_time_incident_transpose` is the transpose of `average_completion_time_by_incident.parquet`
- `incidents_by_zip_transpose` is the transpose of `incidents_per_zip.parquet`

Output files are saved to `hdfs:/user/amr1059/`

## Dataset #3: New Construction Permit Application Data
New Construction Permit Application Data from Open Data NYC
script files:
- `readfile_CP.py
- `cleanfile_CP.py
- `mergzip_CP.py
- `metricCalc_CP.py

additional csv files for input:
- `dfcoordsmR.csv`

output file:
- `DOBsumcostbyzip_job_type.csv
- `dfDOBall.csv

i) The data has already been downloaded from https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD to hdfs directory "hdfs:/user/cp2530/DOB_raw" the file format is csv

ii) To read the data, run:
```
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit readfile_CP.py
```

these commands will read in the file and save it into this directory 'hdfs:/user/cp2530/DOBraw.parquet'


iii) to clean the data, run:
```
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit cleanfile_CP.py 
```

the clearnfile_CP.py script will read in the hdfs:/user/cp2530/DOBraw.parquet, truncate the data to the desired date range, create the job description keywords field


iv) to add the zipcode columns to the dataset, the dfcoordsmR.csv already saved in directory 'hdfs:/user/cp2530/dfcoordsmR.csv' where the mergzip_CP.py will access the file, run: 
```
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit mergzip_CP.py
```
this step will write its output to this directory "hdfs:/user/cp2530/DOBcleanzip.parquet"

v) to calculate the metrics that will be used in the final visualization, run:
```
module load python/gnu/3.6.5
module load spark/2.4.0
alias spark-submit='PYSPARK_PYTHON=$(which python) spark-submit'
spark-submit metricCalc_CP.py
hdfs dfs -getmerge hdfs:/user/cp2530/sumcostbyzip_job_type DOBsumcostbyzip_job_type.csv
```

the output files will be DOBsumcostbyzip_job_type.csv and dfDOBall.csv in your current directory 


## Merging Files
In order to properly visualize our data through Tableau, we needed to join three distinct data sets--valuation data, job construction data, and 311 service data. The following command can be used to merge the aforementioned files `spark-submit file-merger.py hdfs:/user/ctd299/valuation_growth_byzip.csv hdfs:/user/ctd299/valuation_yearly_growth_byzip.csv hdfs:/user/amr1059/dfDOBall.csv hdfs:/user/amr1059/avg_comp_time_transpose.csv`
What `file-merger.py` does is execute left joins to the valuation data as it contains the zipcodes for which we have property value growth measures for.



