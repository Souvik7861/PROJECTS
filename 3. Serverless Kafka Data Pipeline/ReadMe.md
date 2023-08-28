# Kafka Data Pipeline
This project demonstrates how to build a data pipeline using Amazon Managed Streaming for Apache Kafka (Amazon MSK), AWS Lambda, and Amazon Simple Queue Service (SQS).

## Steps
1. Create a VPC.
2. Create two public subnets and two private subnets.
3. Create an Internet gateway and attach it to the VPC.
4. Create two route tables, one for the public subnets and one for the private subnets. 
5. Attach the Internet gateway to the public route tables.
6. Create a NAT gateway and attach it to the private route tables.
7. Launch an Amazon MSK cluster in the private subnets.
8. Create a Lambda function Producer Lambda that publishes messages to the Amazon MSK cluster.
9. Increase the timeout for the Lambda function to 2 minutes.
10. Configure the Lambda function to have access to SQS, Amazon MSK, and the VPC.
11. Launch an SQS queue.
12. Create an API gateway and configure it to integrate with the SQS queue.
13. Create an S3 bucket for data archival.
14. Configure Amazon Kinesis Firehose.
15. Create a Lambda function that consumes messages from Amazon MSK and triggers an Amazon Kinesis Firehose delivery stream, which then writes the messages to Amazon S3.
16. Configure the Lambda function to have access to Amazon Kinesis VPC and MSK.
17. Launch an EC2 instance in each of the public and private subnets.
18. Add the security groups for the EC2 instances and the Amazon MSK cluster to allow all traffic between them.
19. Install Kafka on the EC2 instance in the private subnet.
20. Create a Kafka topic.
21. Start a Kafka console consumer on the EC2 instance in the public subnet.
22. Send some messages to the Lambda function.
23. Verify that the messages are published to the Kafka topic.
24. Verify that the messages are written to S3.


## Dependencies
• AWS CLI   
• Python 3.11   
• Kafka Lambda layer to add in Producer Lambda  
• Boto3 library  

## Code for Lambda Functions


The Producer Lambda function code :

```python
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json

topic_name='{Provide the topic name here}'
producer = KafkaProducer(bootstrap_servers=['{Put the broker URLs here}'
,'{Put the broker URLs here}'],value_serializer=lambda x: dumps(x).encode('utf-8'))

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
