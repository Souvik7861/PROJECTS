# PySpark Application

## Overview
This repository contains a PySpark-based data engineering pipeline for processing and transforming data related to US cities and prescription information. The pipeline performs data ingestion, validation, cleaning, transformation, and persistence into various data storage formats.

## Execution

### Data Ingestion
- **Data Source**: 
    - Data source: `Source/olap/us_cities_dimension.parquet`,`Source/oltp/USA_Presc_Medicare_Data_12021.csv`
- **Loading Data into DataFrame**:
    - Displaying the loaded data into a PySpark DataFrame.
      
![p7 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/78f6fa0d-e644-4d86-9410-31411a3c3182)  

![p7 s2](https://github.com/Souvik7861/PROJECTS/assets/120063616/c03ba5d1-6e2f-4e17-8bed-4ef1efa11dc4)    


### Data Validation and Cleaning    
- **Data Cleaning**:
    - Snapshot of the data after cleaning and preprocessing.    
![p7 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/39b0acfe-14c0-404f-9f00-e3e98f9763ed)

![p7 s4](https://github.com/Souvik7861/PROJECTS/assets/120063616/a8d130ad-9743-4721-863e-549429df0df9)

- **Data Validation**:
    - Checking for null values.
![p7 s5](https://github.com/Souvik7861/PROJECTS/assets/120063616/953ea8ee-91f7-4479-8404-2d66575cbf64)    

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
