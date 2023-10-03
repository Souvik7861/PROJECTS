# Snowflake Snowpark (Python) ETL : Amazon Sales Analytics
### Overview
This repository contains the code and documentation for an end-to-end Data Engineering (DE) project using Snowflake Snowpark and Python. The project focuses on efficiently handling Amazon's mobile sales order data from three regions (India, USA, France) and involves the following stages:

1. Data is moved from an external stage to a source schema.
2. Data is curated in the curated schema.
3. Data is transformed and loaded into the consumption schema.
4. Dashboards are created using the data in the consumption schema.
Data Sources
The Amazon mobile sales data for three regions (India, USA, France) can be found here.

### Data Architecture Diagram
![p6 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/051ebb8d-ca3d-4773-894c-25de670a56d1)

The data files are available in three different formats:

India Sales Order Data: CSV Format  
USA Sales Order Data: Parquet File Format   
France Sales Order Data: JSON File Format   

#### Step 1: Create User & Virtual Warehouse
Before running the ETL workload, you need to create a Snowflake virtual warehouse and a user account.

```sql
-- Create a virtual warehouse
USE ROLE sysadmin;
CREATE WAREHOUSE snowpark_etl_wh 
    WITH 
    WAREHOUSE_SIZE = 'medium' 
    WAREHOUSE_TYPE = 'standard' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = true 
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1 
    SCALING_POLICY = 'standard';

-- Create a Snowpark user (requires accountadmin role)
USER ROLE accountadmin;
CREATE USER snowpark_user 
  PASSWORD = 'Test@12$4' 
  COMMENT = 'This is a Snowpark user' 
  DEFAULT_ROLE = sysadmin
  DEFAULT_SECONDARY_ROLES = ('ALL')
  MUST_CHANGE_PASSWORD = false;

-- Grants
GRANT ROLE sysadmin TO USER snowpark_user;
GRANT USAGE ON WAREHOUSE snowpark_etl_wh TO ROLE sysadmin;
```
**Snowpark Snowflake Connectivity Validation:**    
To validate Snowpark and Snowflake connectivity, configure and run the Snowpark-Snowflake-Connectivity.py file.

#### Step 2: Database & Schema Object
Create the necessary databases and schemas.

```sql
-- Create a database
CREATE DATABASE IF NOT EXISTS sales_dwh;

USE DATABASE sales_dwh;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS source; -- Source stage etc.
CREATE SCHEMA IF NOT EXISTS curated; -- Data curation and de-duplication.
CREATE SCHEMA IF NOT EXISTS consumption; -- Fact & dimension.
CREATE SCHEMA IF NOT EXISTS common; -- For file formats sequence object etc.
```
#### Step 3: External Stage in Source Schema
Create an external stage in the source schema to host all the data from the local machine.  
Refer [storage integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) and [External stage](https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage)
```sql
-- Create storage integration for external stage
CREATE STORAGE INTEGRATION my_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::592777210802:role/snowflake_s3_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://myexternalstg/');

-- Create the external stage
CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = my_s3_int
  URL = 's3://myexternalstg/sales/';
```
-- After creating the external stage, manually put the dataset on the stage (S3 bucket).

#### Step 4: File Format Objects Within Common Schema
Create file formats for reading and processing data from the external stage.

```sql
USE SCHEMA common;

-- Create file formats for CSV (India), JSON (France), Parquet (USA)
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('null', 'null')
  EMPTY_FIELD_AS_NULL = true
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  COMPRESSION = AUTO;

CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = true
  COMPRESSION = AUTO;

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;
```
#### Step 5: Foreign Exchange Rate Data
Create foreign exchange rate data to convert local currency data to US Dollars.

```sql
USE SCHEMA common;

CREATE OR REPLACE TRANSIENT TABLE exchange_rate(
    date DATE, 
    usd2usd DECIMAL(10,7),
    usd2inr DECIMAL(10,7),
    usd2eu DECIMAL(10,7)
);
```
Load the exchange-rate-data.csv dataset manually into the exchange_rate table.

#### Step 6: Create Source sequences and Tables.
Create sequences and source tables for each region to load data from the external stage.

```sql
USE SCHEMA source;

create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';

-- Define table strutures 
-- India Sales Table in Source Schema (CSV File)
create or replace transient table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 mobile varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Source Schema (Parquet File)
create or replace transient table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- France Sales Table in Source Schema (JSON File)
create or replace transient table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);
```
#### Step 7: Snowpark Python Code to Load Data from Stage to Source Schema
Use the stage2source.py Snowpark Python code to load data from the external stage to the source schema.

#### Step 8: Loading Data from Source to Curated Layer
Create sequences and curated tables for each region in the curated schema.

```sql
-- Sequence Object Under Curated Schema Layer
use schema curated;
create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';


-- Curated Layer DDL
use schema curated;
-- curated India sales order table
create or replace table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- curated US sales order table
create or replace table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- -- curated FR sales order table
create or replace table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);
```
#### Step 9: Snowpark Python Code - Source to Curated (IN, US, FR)
Configure and run source2curated_in.py,        source2curated_us.py, and source2curated_fr.py to load data from the source schema to the curated schema for each region.

#### Step 10: Working on Consumption Layer
In the consumption schema, create dimension tables and fact tables.

```sql
-- Region dimension
USE SCHEMA consumption;

CREATE OR REPLACE SEQUENCE region_dim_seq START = 1 INCREMENT = 1;
CREATE OR REPLACE TRANSIENT TABLE region_dim(
    region_id_pk NUMBER PRIMARY KEY,
    Country TEXT, 
    Region TEXT,
    isActive TEXT(1)
);

-- product dimension
create or replace sequence product_dim_seq start = 1 increment = 1;
create or replace transient table product_dim(
    product_id_pk number primary key,
    Mobile_key text,
    Brand text, 
    Model text,
    Color text,
    Memory text,
    isActive text(1)
);


-- promo_code dimension
create or replace sequence promo_code_dim_seq start = 1 increment = 1;
create or replace transient table promo_code_dim(
    promo_code_id_pk number primary key,
    promotion_code  text,
    country text,
    region text,
    isActive text(1)
);


-- customer dimension
create or replace sequence customer_dim_seq start = 1 increment = 1;
create or replace transient table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    CONCTACT_NO text,
    SHIPPING_ADDRESS text,
    country text,
    region text,
    isActive text(1)
);

-- payment dimension
create or replace sequence payment_dim_seq start = 1 increment = 1;
create or replace transient table payment_dim(
    payment_id_pk number primary key,
    PAYMENT_METHOD text,
    PAYMENT_PROVIDER text,
    country text,
    region text,
    isActive text(1)
);

-- date dimension
create or replace sequence date_dim_seq start = 1 increment = 1;
create or replace transient table date_dim(
    date_id_pk int primary key,
    order_dt date,
    order_year int,
    day_counter int,
    oder_month int,
    order_quater int,
    order_day int,
    order_dayofweek int,
    order_dayname text,
    order_dayofmonth int,
    order_weekday text
);


-- fact tables
create or replace sequence sales_fact_seq start = 1 increment = 1;
create or replace table sales_fact (
 order_id_pk number(38,0),
 order_code varchar(),
 date_id_fk number(38,0),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 promo_code_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8)
);



-- Table Containts
alter table sales_fact add
    constraint fk_sales_region FOREIGN KEY (REGION_ID_FK) REFERENCES region_dim (REGION_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_date FOREIGN KEY (DATE_ID_FK) REFERENCES date_dim (DATE_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_customer FOREIGN KEY (CUSTOMER_ID_FK) REFERENCES customer_dim (CUSTOMER_ID_PK) NOT ENFORCED;
--
alter table sales_fact add
    constraint fk_sales_payment FOREIGN KEY (PAYMENT_ID_FK) REFERENCES payment_dim (PAYMENT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_product FOREIGN KEY (PRODUCT_ID_FK) REFERENCES product_dim (PRODUCT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_promot FOREIGN KEY (PROMO_CODE_ID_FK) REFERENCES promo_code_dim (PROMO_CODE_ID_PK) NOT ENFORCED;

```
#### Step 11: Data from Curated to Model (Snowpark Python Code)
Configure and run curated2model.py to load data from the curated schema to the consumption schema in all dimension tables and the fact table.

#### Step 12: Data is Loaded in Consumption Layer and Ready for Use
You can use SQL queries to create dashboards from the data in the consumption schema. Here are some example queries:

```sql 
--Total Sales (USD)
SELECT ROUND(SUM(us_total_order_amt), 0)
FROM consumption.sales_fact sf
JOIN date_dim dd ON sf.date_id_fk = dd.date_id_pk
WHERE dd.ORDER_DT = :datebucket(ORDER_DT);

--Revenue by Country ($)
SELECT rd.country, ROUND(SUM(us_total_order_amt), 0) AS sales_amt
FROM consumption.sales_fact sf
JOIN date_dim dd ON sf.date_id_fk = dd.date_id_pk
JOIN region_dim rd ON sf.region_id_fk = rd.region_id_pk
GROUP BY rd.country;

--Daily Sales Order
SELECT dd.order_day, ROUND(SUM(us_total_order_amt), 0) AS SALES_PER_DAY_IN_USD
FROM consumption.sales_fact sf
JOIN date_dim dd ON sf.date_id_fk = dd.date_id_pk
WHERE dd.ORDER_DT = :datebucket(ORDER_DT)
GROUP BY dd.order_day
ORDER BY dd.order_day;

--Daily Sales Quantity
SELECT dd.order_day, SUM(sf.order_quantity) AS daily_sale_quantity
FROM consumption.sales_fact sf
JOIN date_dim dd ON sf.date_id_fk = dd.date_id_pk
WHERE dd.ORDER_DT = :datebucket(ORDER_DT)
GROUP BY dd.order_day
ORDER BY dd.order_day;
```
![p6 s2](https://github.com/Souvik7861/PROJECTS/assets/120063616/26d82904-9b32-4fcd-827d-f580ace4d149)

Feel free to modify and expand upon these queries to create custom dashboards for your Amazon sales analytics.

For a detailed explanation and to run the Snowpark Python code, refer to the corresponding Python files in this repository.

Please refer to the Snowflake documentation for more information on using Snowflake and Snowpark for your data engineering project.
