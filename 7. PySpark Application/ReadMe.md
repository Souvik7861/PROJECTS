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
        - Before :    
![p7 s6](https://github.com/Souvik7861/PROJECTS/assets/120063616/eaa6da71-5ff5-40ec-8138-b56249edddf7)
        - After :    
![p7 s5](https://github.com/Souvik7861/PROJECTS/assets/120063616/953ea8ee-91f7-4479-8404-2d66575cbf64)    

### Data Transformation
- data_report1 : Which needs number of Zip Codes per city as zipcount and number of distinct prescribers assigned to each city as presc_counts .    
![p7 s7](https://github.com/Souvik7861/PROJECTS/assets/120063616/2b1166a0-7f01-4c61-8a9f-10e0529f3258)    

- data_report2 :
    1) where we need to apply filter on prescribers only fron 20 to 50 years of exp
    2) rank and selecting top 5 prescribers based on their tx_cnt for each State
![p7 s8](https://github.com/Souvik7861/PROJECTS/assets/120063616/59dd98e1-a476-4e64-a0c4-a9ae41d07494)

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
