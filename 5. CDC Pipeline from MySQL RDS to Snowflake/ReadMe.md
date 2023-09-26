# CDC Pipeline from MySQL RDS to Snowflake
![p5 s0](https://github.com/Souvik7861/PROJECTS/assets/120063616/c0bcc944-fd5c-4084-9e6d-34f0d3f5a32d)

## Project Overview
This data engineering project focuses on creating a robust data pipeline for processing and managing data using a variety of AWS services and Snowflake. The project's main objective is to enable real-time data capture, transformation, and storage, making it accessible for analytics and reporting purposes. Here's an overview of the key components and steps involved:

#### Step 1: Create a new RDS Parameter Group
The binary log is a set of log files that contain information about data modifications made to a MySQL server instance.
In the navigation pane, choose Parameter groups & create a new parameter group for MySQL 8.0.
Select the created parameter group, choose Edit.
Set the binlog_format as 'ROW' in the RDS Parameter Group.

#### Step 2: Spin off an RDS
Spin off an RDS (single AZ is sufficient) from the AWS Console.
Note: Use the parameter group created in Step 1 for the RDS you just created.

VVI: The automated backups feature determines whether binary logging is turned on or off for MySQL. You have the following options:

Turn binary logging on: Set the backup retention period to a positive nonzero value.
Turn binary logging off: Set the backup retention period to zero.
#### Step 3: Configure RDS for CDC
Run the following procedure after connecting to RDS MySQL with any SQL client of your choice. It will change the binlog retention duration to 24 hours.
sql
Copy code
CALL mysql.rds_set_configuration('binlog retention hours', 24);
Verification: Run the following SQL to verify that binary logging is enabled:

sql
Copy code
SHOW GLOBAL VARIABLES LIKE "log_bin";
Create the table on which you want to implement CDC:

sql
Copy code
CREATE SCHEMA sattu_schema;
CREATE TABLE sattu_schema.Persons (
    PersonID int,
    FullName varchar(255),
    City varchar(255),
    PRIMARY KEY (PersonID)
);
#### Step 4: Create a Kinesis Stream
Create a Kinesis stream with one shard.
#### Step 5: Create a Kinesis Firehose and Lambda Function
Lambda Code:

```python
import json
import boto3
import base64

output = []

def lambda_handler(event, context):
    print(event)
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        print('payload:', payload)
        
        row_w_newline = payload + "\n"
        print('row_w_newline type:', type(row_w_newline))
        row_w_newline = base64.b64encode(row_w_newline.encode('utf-8'))
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': row_w_newline
        }
        output.append(output_record)

    print('Processed {} records.'.format(len(event['records'])))
    
    return {'records': output}
```
#### Step 6: Create an IAM Role and EC2 Instance
Create an IAM role for EC2 with Kinesis access (e.g., democdc).
Create an EC2 instance.
Execute the following commands on the EC2 instance:
bash
Copy code
sudo apt-get update
sudo apt install python3-virtualenv
virtualenv mysql_test
source mysql_test/bin/activate
sudo apt install python3-pip
python3 -m pip install --upgrade pip
pip install mysql-replication boto3 -t .
For more details about the package, you can refer to this link.

Code reference: AWS Database Blog

Code repository: GitHub Repository

For Tmux:

bash
Copy code
source mysql_test/bin/activate
sudo apt-get install tmux
tmux new -s mysqlcdcdemo
python runner.py
Ctrl+B and then D

#### Step 7: Lambda for Data Processing
From the destination (staging) S3 bucket, trigger a Lambda function that processes the file and calls the stored procedure based on details in each record.
Lambda Code:

```python
import json
import boto3
import snowflake.connector

def lambda_handler(event, context):
    
    # Get the name of the S3 bucket and the key of the file to process.
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Create an S3 client object.
    s3 = boto3.client('s3')
    
    # Get the contents of the S3 file.
    response = s3.get_object(Bucket=bucket_name, Key=key)
    
    # Converting the response into a string and replacing ('') with ("")
    json_data = response['Body'].read().decode('utf-8').replace("'", '"')

    # Making the data iterable
    records = []
    for line in json_data.splitlines():
      records.append(line)

    # Connect to Snowflake.
    snowflake_conn = snowflake.connector.connect(
      user="SOUVIK7861",
      password="Souvik7861@",
      account="yi18169.ap-southeast-1",
      database="CDC_DESTINATION",
      schema="PUBLIC",
      warehouse="COMPUTE_WH"
    )
    
    cursor = snowflake_conn.cursor()
        
    for i in records:
        # Decode the contents of the S3 file into a Python object.
        data = json.loads(i) 
        print(data)
        
        Event_type = data["type"]
        
        if Event_type == "WriteRowsEvent":
            p1 = data["row"]["values"]["PersonID"]
            p2 = data["row"]["values"]["FullName"]
            p3 = data["row"]["values"]["City"]
            sql = "CALL insert_procedure(%s,%s,%s) ;"
            param = (p1, p2, p3)
            cursor.execute(sql, param)
          
        elif Event_type == "DeleteRowsEvent":
            p1 = data["row"]["values"]["PersonID"]
            sql = "CALL delete_procedure(%s) ;"
            param = (p1,)
            cursor.execute(sql, param)
          
        elif Event_type == "UpdateRowsEvent":
            p1 = data["row"]["after_values"]["PersonID"]
            p2 = data["row"]["after_values"]["FullName"]
            p3 = data["row"]["after_values"]["City"]
            sql = "CALL update_procedure(%s,%s,%s) ;"
            param = (p1, p2, p3)
            cursor.execute(sql, param)
          
    cursor.close()
    # Close the Snowflake connection.
    snowflake_conn.close()

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
```    
These steps outline the setup and configuration required for your data engineering project. Follow them carefully to ensure that your project functions as intended.