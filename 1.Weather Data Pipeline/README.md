# Weather Data Pipeline
![image](https://github.com/Souvik7861/PROJECTS/assets/120063616/e3ba62ba-1a5b-4c67-8172-826caa900a64)


![image](https://github.com/Souvik7861/PROJECTS/assets/120063616/6e17efb8-39a9-4282-ad7b-005e0b6c6443)

This project implements a data pipeline to extract weather data from an API, transform it, and load it into a PostgreSQL database table named 'weather_data.' Additionally, it extracts city lookup data from S3, loads it into another PostgreSQL table named 'city_look_up,' and finally performs a join operation on both tables to create a joined dataset. The joined data is then loaded back into S3.

## Data Pipeline
The data pipeline consists of the following steps:

1. Dummy Operation:

•The pipeline starts with a dummy operation that serves as a       trigger for the subsequent tasks.

2. Parallel Processing Group Task:

• This task is divided into two parallel branches.

2.1 API Data Extraction, Transformation, and Load:

• In one branch of the parallel task, data is extracted from an API source.
The extracted data is then transformed into a suitable format for analysis.
The transformed data is loaded into the 'weather_data' table in the PostgreSQL database.

2.2 S3 Data Extraction and Load:

• In the other branch of the parallel task, data is extracted from S3 storage.
The data is directly loaded into the 'city_look_up' table in the PostgreSQL database using aws_s3.table_import_from_s3.

3. Joining Data and Loading to S3:

• After both branches of the parallel task are completed, the data from the 'weather_data' and 'city_look_up' tables are joined together based on a common key.
The joined data is then loaded back into S3 storage.

## Setup Instructions

To run this project, follow these steps:                        

1. Set up the PostgreSQL database and tables:

• Create a PostgreSQL RDS in AWS.  
• Create a task in dag which creates the 'weather_data' table with appropriate columns to store the weather data.    
• Create a task in dag which creates the 'city_look_up' table with appropriate columns to store the city lookup data.    

2. Set up AWS credentials:

• Obtain AWS credentials (access key and secret key) to access S3 storage and ensure they have sufficient permissions.

3. Update the configuration file:

•Edit the configuration file to specify the API source details, S3 bucket information, PostgreSQL database connection details, and other necessary parameters. 

4. Install dependencies:

• Install the required libraries and packages as specified in the project's requirements/Dependencies file.

5. Run the data pipeline:

• Execute the main script that orchestrates the data pipeline.  
•The pipeline will start with the dummy operation and then proceed with the parallel processing tasks, extracting data from the API and S3, performing transformations, loading data into the database, and finally joining and exporting the results back to S3.

## Dependencies

To set up the project and run the data pipeline, you need the following dependencies :

1. Update system packages:
   ```bash
   sudo apt update

2. Install Python3 and pip:
   ```bash
   sudo apt install python3-pip


3. Install Python3 virtual environment:
   ```bash
   sudo apt install python3.10-venv
   python3 -m venv airflow_venv

4. Install required Python libraries:
   ```bash
    sudo pip install pandas
    sudo pip install s3fs
    sudo pip install fsspec
    sudo pip install apache-airflow
    sudo pip install apache-airflow-providers-postgres

5. Install PostgreSQL in EC2 :
    https://www.postgresql.org/download/linux/ubuntu/
    ```bash
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update
    sudo apt-get -y install postgresql

6. Enable AWS S3 Extension in PostgreSQL:            

• Connect to the PostgreSQL database:
```bash
psql --host={Your rds endpoint} --port=5432 --username=postgres --password 
```  

• Inside PostgreSQL shell, run the following commands:
```sql
postgres=> CREATE EXTENSION aws_s3 CASCADE;
```
7. Install AWS CLI & Configure AWS CLI:
```bash
sudo apt install awscli

aws configure
[Enter your AWS access key, secret access key, region, and output format]
```
8. •Create IAM Policy   
•Role for RDS S3 Import     
•attach IAM Policy to IAM Role  
•Add IAM Role to RDS Database Instance

Do the above tasks by following the link:https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html



