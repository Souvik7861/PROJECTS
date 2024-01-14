# PySpark EMR Airflow: Real Estate Transformations
## Overview
This project creates an ETL pipeline that extracts data from a source, transforms it using a temporary EMR cluster, and loads it into a Snowflake database for visualization in Power BI. The pipeline is orchestrated using Apache Airflow.  

![p9 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/3274e167-2001-43ab-aaad-92dfc548a95b)

## Architecture Overview
### Environment Setup
- Create an EC2 instance and install required dependencies and configure it to run Apache Airflow .
```bash
python3 --version
sudo apt update
sudo apt install python3-pip
sudo apt install python3.10-venv
python3 -m venv redfin_venv
source redfin_venv/bin/activate
pip install boto3
pip install --upgrade awscli
aws configure
pip install apache-airflow
pip install apache-airflow-providers-amazon
airflow version
airflow standalone
aws iam list-roles | grep 'EMR_DefaultRole\|EMR_EC2_DefaultRole'
aws emr create-default-roles
```
- Submit the DAG file (redfin_analytics.py) in the designated DAGs folder, which is typically configured in airflow.cfg ( ~/airflow/airflow.cfg ) under the dags_folder setting.

### Pipeline Steps
![p9 s2](https://github.com/Souvik7861/PROJECTS/assets/120063616/df5b6c1b-db2a-490f-bc9d-69c124cdece2)

- Start Pipeline: Initiates the DAG execution.
- Create EMR Cluster: Dynamically provisions an EMR cluster for processing.
- Is EMR Cluster Created: Checks for successful cluster creation.
- Add Extraction Step: Submits an extraction job to the EMR cluster.
- Is Extraction Completed: Monitors the extraction job's completion.
  ![p9 s8](https://github.com/Souvik7861/PROJECTS/assets/120063616/5200d0d3-1bcf-4ac5-886f-a6682452034f)
- Add Transformation Step: Submits a transformation job to the EMR cluster.
- Is Transformation Completed: Monitors the transformation job's completion.
  ![p9 s9](https://github.com/Souvik7861/PROJECTS/assets/120063616/d3fe9718-78b3-472d-a175-bebd113c9a17)
- Remove Cluster: Terminates the EMR cluster to optimize costs.
  ![p9 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/454d706f-3c83-4690-836e-11edcd5e84ae)
- Is EMR Cluster Terminated: Ensures successful cluster termination.
- End Pipeline: Marks DAG completion.
![p9 s4](https://github.com/Souvik7861/PROJECTS/assets/120063616/2b6ea42f-43f1-4e16-8667-585b7c830c54)

  
### Data Loading (Snowpipe to Snowflake): 
- Create table with Schema of the transformed data 
- Create External stage with Storage Integration to the target directory in s3 bucket.
- Create SnowPipe with auto_ingests "True" , which copies the data from target directory into the desired Table. (The AUTO_INGEST=true parameter specifies to read event notifications sent from an S3 bucket to an SQS queue when new data is ready to load)
  ![p9 s11](https://github.com/Souvik7861/PROJECTS/assets/120063616/b0b7a9ed-e25c-4857-b577-939ac3a1974b)
- Test the Pipeline by Rerunning the Dag in Airflow and see if the data is loaded in desired table after transformation or not.
  ![p9 s13](https://github.com/Souvik7861/PROJECTS/assets/120063616/63024df2-68a7-4f56-9f43-9fc72a3c223a)

### Data Visualization (Power BI):  
- Access Snowflake data and Load it in Power BI.  
![p9 s14](https://github.com/Souvik7861/PROJECTS/assets/120063616/77af8817-5c16-4b8c-9c22-21097f051e9e)  
![p9 s15](https://github.com/Souvik7861/PROJECTS/assets/120063616/90e3c282-f4b9-4f7e-819a-a05e590e4d82)
- After loading data in powerbi , create visualization.
![p9 s16](https://github.com/Souvik7861/PROJECTS/assets/120063616/7659888f-150a-46b6-aa9d-032e7dfa643a)



