# PySpark Application

## Overview
This repository contains a PySpark-based data engineering pipeline for processing and transforming data related to US cities and prescription information. The pipeline performs data ingestion, validation, cleaning, transformation, and persistence into various data storage formats.

## Execution

### Data Ingestion
- **Data Source**: 
    - Data source: `us_cities_dimension.parquet`
    - Path: `C:\Users\souvik\PycharmProjects\Pyspark_realtime_application\Source\olap\us_cities_dimension.parquet`
- **Loading Data into DataFrame**:
    - Displaying the loaded data into a PySpark DataFrame.

![Data Ingestion](/images/data_ingestion.png)

### Data Validation and Cleaning
- **Data Validation**:
    - Checking for null values.
- **Data Cleaning**:
    - Snapshot of the data after cleaning and preprocessing.

![Data Validation and Cleaning](/images/data_validation_cleaning.png)

### Data Transformation
- Transformations applied, such as column renaming or specific operations.
- Snapshot of the transformed data.

![Data Transformation](/images/data_transformation.png)

### Data Persistence
- Saving the data into Hive tables.
- Saving the data into MySQL tables.

![Data Persistence](/images/data_persistence.png)

### Data Reports
- Displaying generated reports, which may include tables, charts, or other visual representations of the data.

![Data Reports](/images/data_reports.png)

### Logging and Execution Time
- Log file displaying the execution flow and any error messages.
- Total execution time of the pipeline.

![Logging and Execution Time](/images/logging_execution_time.png)

---

For more details and code implementation, refer to `driver.py` and other relevant scripts in this repository.
