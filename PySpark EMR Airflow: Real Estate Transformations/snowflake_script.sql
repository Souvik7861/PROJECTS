-- DROP DATABASE IF EXISTS redfin_database_1;
CREATE DATABASE redfin_database_1;
CREATE SCHEMA redfin_schema;
// Create Table
--TRUNCATE TABLE redfin_database_1.redfin_schema.redfin_table;
CREATE OR REPLACE TABLE redfin_database_1.redfin_schema.redfin_table (
    city STRING,
    homes_sold INT,
    inventory INT,
    median_dom INT,
    median_ppsf FLOAT,
    median_sale_price FLOAT,
    months_of_supply FLOAT,
    period_duration INT,
    period_end_month STRING,
    period_end_yr INT,
    property_type STRING,
    sold_above_list FLOAT,
    state STRING   
);



// Create file format object
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format redfin_database_1.file_format_schema.format_PARQUET
    TYPE = parquet;

    
    
// Create STORAGE INTEGRATION 
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::592777210802:role/snowflake_s3_pyspark_emr_airflow_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://pyspark-emr-airflow-yml/') ;
DESC INTEGRATION s3_int;



// Create staging schema
CREATE SCHEMA external_stage_schema;
// Create staging
CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://pyspark-emr-airflow-yml/transformed-data-yml/'
  FILE_FORMAT = redfin_database_1.file_format_schema.format_PARQUET;


  
// Create schema for snowpipe
-- DROP SCHEMA redfin_database.snowpipe_schema;
CREATE OR REPLACE SCHEMA redfin_database_1.snowpipe_schema;
// Create Snow Pipe
CREATE OR REPLACE PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe
auto_ingest = TRUE
AS 
COPY INTO redfin_database_1.redfin_schema.redfin_table
FROM @redfin_database_1.external_stage_schema.my_s3_stage/
FILE_FORMAT = (TYPE = PARQUET) 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ;

DESC PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe;



SELECT * FROM redfin_database_1.redfin_schema.redfin_table limit 10 ;








