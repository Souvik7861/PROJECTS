# PySpark EMR Airflow: Real Estate Transformations
## Overview
This data engineering project revolves around extracting, transforming, and loading real estate data from Redfin using PySpark (using AWS EMR) , orchestrated with Apache Airflow. The processed data is then loaded into Snowflake, enabling seamless integration with Power BI for visualization.  

![p9 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/3274e167-2001-43ab-aaad-92dfc548a95b)

## Architecture Overview
### Data Extraction (PySpark on AWS EMR):
- Extract Redfin data using PySpark on Amazon EMR.  
- Logs stored in the specified S3 bucket.
### Data Transformation (PySpark on AWS EMR):
- Transform the data using PySpark.  
- Apache Airflow manages the workflow and dependencies.  
- Logs captured in EMR logs on S3.  
### Data Loading (Snowpipe to Snowflake):  
- Load transformed data back to an S3 bucket.  
- Snowpipe triggers the loading of new data into the Snowflake data warehouse.  
### Data Visualization (Power BI):  
- Access Snowflake data for visualization in Power BI.  

