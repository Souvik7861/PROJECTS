# Automated Serverless DataLake using an AWS Glue , Lambda , Cloudwatch

## Overview
This project is a data engineering pipeline designed to automate the process of ingesting, processing, and notifying users about data transformations. It leverages AWS services such as S3, Lambda, Glue, EventBridge, and SNS to achieve this.

## Architecture
Here's an overview of the architecture:
![p4 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/d9e97a03-d2b5-4c6a-bff3-2eef18eeeecf)

1. S3 Bucket for Raw Data: Data files are uploaded to an S3 bucket.
![p4 s2](https://github.com/Souvik7861/PROJECTS/assets/120063616/e8c71868-cc2b-4550-9721-a49a50d8f437)

2. Lambda Trigger: When files are uploaded to the S3 bucket, a Lambda function is triggered.
![p4 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/117aa60f-0076-4b6c-b261-53f28f19a107)

3. Glue Crawler: The Lambda function triggers a Glue Crawler, which crawls over the raw data and creates a metadata catalog.
![p4 s4](https://github.com/Souvik7861/PROJECTS/assets/120063616/3454221b-9ac0-433f-9f49-09b19b715fec)

4. CloudWatch Event Rule (EventBridge): After the Glue Crawler has succeeded, a CloudWatch Event Rule (EventBridge) is triggered.
![p4 s5](https://github.com/Souvik7861/PROJECTS/assets/120063616/c1a10159-a2b0-44b0-964e-714b107ca1c3)

5. Notification Lambda: The CloudWatch Event Rule triggers a Lambda function.
![p4 s6](https://github.com/Souvik7861/PROJECTS/assets/120063616/89b3b623-3816-43be-8a4d-a2a078580213)

6. Glue ETL Job: The Lambda function triggers a Glue ETL job, which extracts data from the Glue Data Catalog table, transforms it, and loads it into a target S3 bucket.
![p4 s7](https://github.com/Souvik7861/PROJECTS/assets/120063616/41352bcd-3481-4814-a0cd-7e76c5f96fb0)
![p4 s10](https://github.com/Souvik7861/PROJECTS/assets/120063616/23051bb3-57d3-49a2-b240-a21197952f7f)

7. SNS Notification: Upon the successful completion of the Glue ETL job (when its state is "SUCCEEDED"), a CloudWatch Event (EventBridge) rule with the appropriate event pattern is triggered.
![p4 s8](https://github.com/Souvik7861/PROJECTS/assets/120063616/baa6272a-f868-4feb-b3b5-aefd03a92370)

8. SNS Notification to Subscribers: The CloudWatch Event Rule sends an SNS notification to subscribers to inform them that the job is complete.
![p4 s11](https://github.com/Souvik7861/PROJECTS/assets/120063616/21899145-9991-4f80-8b36-4a20f0658a47)

## Setup Instructions
To set up and run this data engineering project, follow these steps:


### Deployment

1. Create an S3 bucket for storing raw data and a target S3 bucket for processed data. Note their names and update the configuration files accordingly.

2. Create Glue Data Catalog databases and tables as needed for your data.

3. Deploy the Lambda functions (lambda-trigger, notification-lambda) using AWS Lambda.

4. Set up the necessary IAM roles and permissions for the Lambda functions, Glue, and S3 buckets.

5. Create Glue ETL jobs for data transformation and loading.

6. Configure CloudWatch Event Rules (EventBridge) to trigger Lambda functions based on the events described in the architecture.

7. Set up SNS topics and subscribers for notifications.

### Usage
1. Upload raw data files to the S3 bucket designated for raw data.

2. Monitor the AWS services and logs to ensure that each step in the pipeline executes as expected.

3. When the Glue ETL job completes successfully (state is "SUCCEEDED"), subscribers will receive SNS notifications.
