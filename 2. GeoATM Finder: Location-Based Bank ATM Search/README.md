
# GeoATM Finder: Location-Based Bank ATM Search

![p2 s1](https://github.com/Souvik7861/PROJECTS/assets/120063616/3c1ac2ec-c825-4816-87e5-5a456e71cfaa)


This repository contains a data engineering project that focuses on creating a location-based search service for bank ATM locations using Amazon DynamoDB, AWS Lambda, and Algolia. The project allows users to search for nearby bank ATM locations based on their complete address and retrieve relevant results.

### Project Overview
The project involves several steps to set up the complete system for location-based ATM search:

#### Step 1: Create DynamoDB Table and Enable Stream

• Create a DynamoDB table named banklocation with locationId as the primary key (partition key).   
• Enable DynamoDB Streams for the banklocation table to capture data changes.

#### Step 2: Lambda Function to Index Data in Algolia

• Implement a Lambda function that is triggered by DynamoDB Streams when new data is inserted.  
• The Lambda function extracts relevant information from the DynamoDB event, constructs a complete address, and then uses Algolia to index the data for efficient location-based search.  
• The Lambda function code can be found in the Lambda_code.txt file.    
![p2 s3](https://github.com/Souvik7861/PROJECTS/assets/120063616/6283919c-0019-4901-964f-0a54ca40f0cc)

#### Step 3: Configure Lambda Role and Trigger

• Grant the Lambda function the necessary permissions to access DynamoDB and the AWS Location Service.   
• Create a DynamoDB trigger to invoke the Lambda function whenever new data is inserted into the banklocation table.

#### Step 4: Create Lambda Layer for Algolia

• Set up a Lambda Layer containing the algoliasearch library to be used by the Lambda function.    
• Detailed instructions for creating and uploading the Lambda Layer can be found in the lambda_layers directory.

#### Step 5: Run DynamoDB Queries to Ingest Data

• Insert sample ATM location data into the banklocation DynamoDB table using SQL-like queries.  
• This step populates the DynamoDB table with sample data for testing and demonstration. 
![p2 s2](https://github.com/Souvik7861/PROJECTS/assets/120063616/00474e28-5a11-446a-ab19-2478634002bd)

#### Step 6: Implement Location-Based Search Lambda

•Develop another Lambda function that performs location-based search using the Algolia index.   
•This Lambda function takes a complete address as input and returns nearby ATM locations.    
•The Lambda function code for location-based search can be found in the Lambda_code.txt file.

#### Step 7: Create API Gateway

• Set up an API using AWS API Gateway to expose the location-based search functionality.  
•The API Gateway routes incoming requests to the location search Lambda function.
![p2 s4](https://github.com/Souvik7861/PROJECTS/assets/120063616/2e4eadeb-84c9-4773-a296-33e0958a91dd)

#### Step 8: Perform End-to-End Testing

• Test the complete system end-to-end using sample test cases.    
•This step ensures that the location-based search service works as expected.

### Testing the Location-Based Search
To test the location-based search functionality, you can use the following test cases:

Search for ATM locations near "Kalka chownk, New Vita Enclave Rd, near vita milk plant, Ambala, Haryana 134003":
![p2 s5](https://github.com/Souvik7861/PROJECTS/assets/120063616/9356e1c5-01d9-4efd-885b-ed64439d0a45)

Search for ATM locations near " Range hills, near khadki , Pune, Maharashtra 411020": 
![p2 s6](https://github.com/Souvik7861/PROJECTS/assets/120063616/f9c4e3d7-ec4d-4e78-bf79-354c6eb851bf)

Search for ATM locations near "House no 2, Mother teresa road, Geetanagar, Auditek, bylane, Guwahati, Assam 781020":
![p2 s7](https://github.com/Souvik7861/PROJECTS/assets/120063616/1c3fc548-dafb-4691-b89c-0c0471935b3a)

Search for ATM locations near "North, 400, Grand Trunk Rd, Salkia, Howrah, West Bengal 711101":
![p2 s8](https://github.com/Souvik7861/PROJECTS/assets/120063616/9d79524d-b86e-475e-9cd8-6d1ec6700d1e)


### Installation and Setup

Follow the steps outlined in each section to set up and configure the project components, including DynamoDB, Lambda functions, Algolia, API Gateway, and testing.


Note: Replace placeholders such as {App Name}, {API Key}, {Index/Algolia Table Name}, {Place Index name}, and [Your Name] with appropriate values and details.

