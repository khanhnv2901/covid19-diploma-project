
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# read data from table of glue cata log
enigma_jhu_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='enigma_jhu')
nytimes_data_in_usa_us_county_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='nytimes_data_in_usa_us_county')
nytimes_data_in_usa_us_states_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='nytimes_data_in_usa_us_states')
rearc_covid19_testing_states_daily_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='rearc_covid19_testing_states_daily')
rearc_covid19_testing_us_daily_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='rearc_covid19_testing_us_daily')
rearc_covid19_testing_us_total_latest_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='rearc_covid19_testing_us_total_latest')
rearc_usa_hospital_beds_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='rearc_usa_hospital_beds')
static_dataset_countrycode_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='static_dataset_countrycode')
static_dataset_countypopulation_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='static_dataset_countypopulation')
static_dataset_state_abv_dyf = glueContext.create_dynamic_frame.from_catalog(database='covid_19', table_name='static_dataset_state_abv')
# change to spark dataframe
enigma_jhu_df = enigma_jhu_dyf.toDF()
nytimes_data_in_usa_us_county_df = nytimes_data_in_usa_us_county_dyf.toDF()
nytimes_data_in_usa_us_states_df = nytimes_data_in_usa_us_states_dyf.toDF()
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_dyf.toDF()
rearc_covid19_testing_us_daily_df =rearc_covid19_testing_us_daily_dyf.toDF()
rearc_covid19_testing_us_total_latest_df = rearc_covid19_testing_us_total_latest_dyf.toDF()
rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_dyf.toDF()
static_dataset_countrycode_df = static_dataset_countrycode_dyf.toDF()
static_dataset_countypopulation_df = static_dataset_countypopulation_dyf.toDF()
static_dataset_state_abv_df = static_dataset_state_abv_dyf.toDF()
enigma_jhu_df.show(5)
from pyspark.sql.functions import when, length, concat, col, format_string
# fips code of county-level
# and then add '0' before 4 digit fips code
# and only take rows that have 5 digit fips and in range(01000, 56045) 
enigma_jhu_df = enigma_jhu_df.withColumnRenamed('fips', 'old_fips') \
    .withColumn('fips', format_string("%05d", col('old_fips').cast('int'))) \
    .drop('old_fips') \
    .filter((col('fips').rlike('^[0-9]{5}$')) & (col('fips') >= '01000') & (col('fips') <= '56045'))

nytimes_data_in_usa_us_county_df = nytimes_data_in_usa_us_county_df.withColumnRenamed('fips', 'old_fips') \
    .withColumn('fips', format_string("%05d", col('old_fips').cast('int'))) \
    .drop('old_fips') \
    .filter((col('fips').rlike('^[0-9]{5}$')) & (col('fips') >= '01000') & (col('fips') <= '56045'))

rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_df.withColumnRenamed('fips', 'old_fips') \
    .withColumn('fips', format_string("%05d", col('old_fips').cast('int'))) \
    .drop('old_fips') \
    .filter((col('fips').rlike('^[0-9]{5}$')) & (col('fips') >= '01000') & (col('fips') <= '56045'))
                    
enigma_jhu_df.show(10)
# rename 'admin2' column to 'county' column
enigma_jhu_df = enigma_jhu_df.withColumnRenamed('admin2', 'county')
enigma_jhu_df.show(10)
# now check the nytimes_data_in_usa_us_county_df 
nytimes_data_in_usa_us_county_df.show(10)
# check rearc_usa_hospital_beds_df
rearc_usa_hospital_beds_df.show(10)
# validate 5 digit zip code
rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_df.withColumnRenamed('hq_zip_code', 'old_hq_zip_code') \
    .withColumn('hq_zip_code', format_string("%05d", col('old_hq_zip_code').cast('int'))) \
    .drop('old_hq_zip_code')
# drop column hq_address1
rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_df.drop('hq_address1')
rearc_usa_hospital_beds_df.show(5)
enigma_jhu_df = enigma_jhu_df.withColumnRenamed('fips', 'county_fips')
nytimes_data_in_usa_us_county_df = nytimes_data_in_usa_us_county_df.withColumnRenamed('fips', 'county_fips')
rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_df.withColumnRenamed('fips', 'county_fips')
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.withColumn('state_fips', format_string("%02d", col('fips').cast('int'))) \
    .drop('fips') \
    .filter((col('state_fips').rlike('^[0-9]{2}$')) & (col('state_fips') >= '01') & (col('state_fips') <= '56'))
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.withColumn('state_fips', format_string("%02d", col('states').cast('int'))) \
    .drop('states') \
    .filter((col('state_fips').rlike('^[0-9]{2}$')) & (col('state_fips') >= '01') & (col('state_fips') <= '56'))

rearc_covid19_testing_us_daily_df.select('state_fips').show(10)
from pyspark.sql.functions import to_date, date_format, col, to_timestamp 
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.withColumn('date_str', col('date').cast('string'))
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.withColumn('timestamp',  to_timestamp("date_str", "yyyyMMdd"))
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.withColumn('date', date_format('timestamp', 'yyyy-MM-dd'))
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.drop('date_str')
rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.drop('timestamp')
rearc_covid19_testing_states_daily_df.select('date').show(5)
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.withColumn('date_str', col('date').cast('string'))
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.withColumn('timestamp',  to_timestamp("date_str", "yyyyMMdd"))
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.withColumn('date', date_format('timestamp', 'yyyy-MM-dd'))
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.drop('date_str')
rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.drop('timestamp')
rearc_covid19_testing_us_daily_df.select('date').show(5)
nytimes_data_in_usa_us_states_df.select('date').show(5)
nytimes_data_in_usa_us_county_df.select('date').show(5)
from pyspark.sql.functions import isnull
for col_name in rearc_usa_hospital_beds_df.columns:
    # Check for null values
    if rearc_usa_hospital_beds_df.filter(col(col_name).isNull()).count() > 0:
        # Replace null values with 0
        rearc_usa_hospital_beds_df = rearc_usa_hospital_beds_df.fillna({col_name: 0})
for col_name in rearc_covid19_testing_states_daily_df.columns:
    # Check for null values
    if rearc_covid19_testing_states_daily_df.filter(col(col_name).isNull()).count() > 0:
        # Replace null values with 0
        rearc_covid19_testing_states_daily_df = rearc_covid19_testing_states_daily_df.fillna({col_name: 0})
for col_name in rearc_covid19_testing_us_daily_df.columns:
    # Check for null values
    if rearc_covid19_testing_us_daily_df.filter(col(col_name).isNull()).count() > 0:
        # Replace null values with 0
        rearc_covid19_testing_us_daily_df = rearc_covid19_testing_us_daily_df.fillna({col_name: 0})
# Import Dynamic DataFrame class
from awsglue.dynamicframe import DynamicFrame
#Convert from Spark Data Frame to Glue Dynamic Frame
enigma_jhu_dyf = DynamicFrame.fromDF(enigma_jhu_df, glueContext, "convert")
nytimes_data_in_usa_us_county_dyf = DynamicFrame.fromDF(nytimes_data_in_usa_us_county_df, glueContext, "convert")
nytimes_data_in_usa_us_states_dyf = DynamicFrame.fromDF(nytimes_data_in_usa_us_states_df, glueContext, "convert")
rearc_covid19_testing_states_daily_dyf = DynamicFrame.fromDF(rearc_covid19_testing_states_daily_df, glueContext, "convert")
rearc_covid19_testing_us_daily_dyf = DynamicFrame.fromDF(rearc_covid19_testing_us_daily_df, glueContext, "convert")
rearc_covid19_testing_us_total_latest_dyf = DynamicFrame.fromDF(rearc_covid19_testing_us_total_latest_df, glueContext, "convert")
rearc_usa_hospital_beds_dyf = DynamicFrame.fromDF(rearc_usa_hospital_beds_df, glueContext, "convert")
static_dataset_countrycode_dyf = DynamicFrame.fromDF(static_dataset_countrycode_df, glueContext, "convert")
static_dataset_countypopulation_dyf = DynamicFrame.fromDF(static_dataset_countypopulation_df, glueContext, "convert")
static_dataset_state_abv_dyf = DynamicFrame.fromDF(static_dataset_state_abv_df, glueContext, "convert")
# write down the data in converted Dynamic Frame to S3 location.
enigma_jhu_dyf = enigma_jhu_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = enigma_jhu_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
# write down the data in converted Dynamic Frame to S3 location.
nytimes_data_in_usa_us_county_dyf = nytimes_data_in_usa_us_county_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = nytimes_data_in_usa_us_county_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
# write down the data in converted Dynamic Frame to S3 location.
nytimes_data_in_usa_us_states_dyf = nytimes_data_in_usa_us_states_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = nytimes_data_in_usa_us_states_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
# write down the data in converted Dynamic Frame to S3 location.
rearc_covid19_testing_states_daily_dyf = rearc_covid19_testing_states_daily_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = rearc_covid19_testing_states_daily_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
# write down the data in converted Dynamic Frame to S3 location.
rearc_covid19_testing_us_daily_dyf = rearc_covid19_testing_us_daily_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = rearc_covid19_testing_us_daily_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
rearc_covid19_testing_us_total_latest_dyf = rearc_covid19_testing_us_total_latest_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = rearc_covid19_testing_us_total_latest_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
rearc_usa_hospital_beds_dyf = rearc_usa_hospital_beds_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = rearc_usa_hospital_beds_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
static_dataset_countrycode_dyf = static_dataset_countrycode_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = static_dataset_countrycode_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
static_dataset_countypopulation_dyf  = static_dataset_countypopulation_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = static_dataset_countypopulation_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
static_dataset_state_abv_dyf = static_dataset_state_abv_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
                            frame = static_dataset_state_abv_dyf,
                            connection_type="s3", 
                            connection_options = {
                                "path": "s3://khanhnv-covid19-result-bucket/output_result/",
                                "partitionKeys": []
                            }, 
                            format = "csv", 
                            format_options={
                                "separator": ","
                                },
                            transformation_ctx = "datasink2")
job.commit()