# Serverless Kafka Data Pipeline
![p3 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/45cffae8-ffff-4536-9511-c1d547678c68)

This project demonstrates how to build a data pipeline using Amazon Managed Streaming for Apache Kafka (Amazon MSK), AWS Lambda, and Amazon Simple Queue Service (SQS).

## Steps
1. Create a VPC.
2. Create two public subnets and two private subnets.
3. Create an Internet gateway and attach it to the VPC.
4. Create two route tables, one for the public subnets and one for the private subnets. 
5. Attach the Internet gateway to the public route tables.
6. Create a NAT gateway and attach it to the private route tables.
![p3 s11](https://github.com/Souvik7861/PROJECTS/assets/120063616/bfe5daf3-f33b-4192-a61f-7491d496087b)
7. Launch an Amazon MSK cluster in the private subnets.
![p3 s12](https://github.com/Souvik7861/PROJECTS/assets/120063616/5354bc4a-a139-401c-8209-cb77ceaafec7)

8. Create a Lambda function Producer Lambda that publishes messages to the Amazon MSK cluster.
![p3 s4](https://github.com/Souvik7861/PROJECTS/assets/120063616/53a3ddfb-4aee-403e-a285-6412e72c3811)
9. Configure the Lambda function to have access to SQS, Amazon MSK, and the VPC.
10. Launch an SQS queue.![p3 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/a5be47d2-66f4-40c9-af9b-e1f9af2db2d3)

11. Create an API gateway and configure it to integrate with the SQS queue.
12. Create an S3 bucket for data archival.
13. Configure Amazon Kinesis Firehose.
14. Create a Lambda function that consumes messages from Amazon MSK and triggers an Amazon Kinesis Firehose delivery stream, which then writes the messages to Amazon S3.![p3 s6](https://github.com/Souvik7861/PROJECTS/assets/120063616/06d766f8-9d35-4456-afb5-35800d9dbf63)

15. Configure the Lambda function to have access to Amazon Kinesis , VPC and MSK.
16. Launch an EC2 instance in each of the public and private subnets.
17. Add the security groups for the EC2 instances and the Amazon MSK cluster to allow all traffic between them.
18. Install Kafka on the EC2 instance in the private subnet.
19. Create a Kafka topic.
20. Start a Kafka console consumer on the EC2 instance in the public subnet.
21. Test the data pipeline with external application to post data through api gateway.
![p3 s10](https://github.com/Souvik7861/PROJECTS/assets/120063616/014b0b16-f9ab-4188-bc27-090dcd2e6c88)
22. Verify that the messages are published to the Kafka topic.

![p3 s5](https://github.com/Souvik7861/PROJECTS/assets/120063616/b96a2bc9-2ed8-4800-9b88-0877be9baf5b)

23. Verify that the messages are written to S3.
![p3 s8](https://github.com/Souvik7861/PROJECTS/assets/120063616/bfdef04d-6beb-4fe4-a9f0-0cd59e129f44)
24. Verify the sample S3 files content.
    
![p3 s9](https://github.com/Souvik7861/PROJECTS/assets/120063616/83b97412-ee35-4c9f-b34d-dc213123bb5f)


## Dependencies
• AWS CLI   
• Python 3.11   
• Kafka Lambda layer to add in Producer Lambda  
• Boto3 library  

## Code for Lambda Functions

Code to create the Lambda Layer:
```bash
sudo apt-get update
sudo apt install python3-virtualenv
virtualenv kafka_Layer
source kafka_Layer/bin/activate
python3 --version  
sudo apt install python3-pip
python3 -m pip install --upgrade pip
mkdir -p lambda_layers/python/lib/python3.11/site-packages
cd lambda_layers/python/lib/python3.11/site-packages
pip install  kafka-python -t .
cd ~
sudo apt install zip
zip -r kafka_Layer.zip *
```

The Producer Lambda function code :

```python
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json

topic_name='{Provide the topic name here}'
producer = KafkaProducer(bootstrap_servers=['{Put the broker URLs here}','{Put the broker URLs here}'],value_serializer=lambda x: dumps(x).encode('utf-8'))

def lambda_handler(event, context):
    print(event)
    for i in event['Records']:
        sqs_message =json.loads((i['body']))
        print(sqs_message)
        producer.send(topic_name, value=sqs_message)
    
    producer.flush()
```
The Consumer Lambda function code :
```python
import base64
import boto3
import json

client = boto3.client('firehose')

def lambda_handler(event, context):
	print(event)
	for partition_key in event['records']:
		partition_value=event['records'][partition_key]
		for record_value in partition_value:
			actual_message=json.loads((base64.b64decode(record_value['value'])).decode('utf-8'))
			print(actual_message)
			newImage = (json.dumps(actual_message)+'\n').encode('utf-8')
			print(newImage)
			response = client.put_record(
			DeliveryStreamName='{Kinesis Delivery Stream Name}',
			Record={
			'Data': newImage
			})
```
