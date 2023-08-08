
# GeoATM Finder: Location-Based Bank ATM Search

This repository contains a data engineering project that focuses on creating a location-based search service for bank ATM locations using Amazon DynamoDB, AWS Lambda, and Algolia. The project allows users to search for nearby bank ATM locations based on their complete address and retrieve relevant results.

### Project Overview
The project involves several steps to set up the complete system for location-based ATM search:

#### Step 1: Create DynamoDB Table and Enable Stream

• Create a DynamoDB table named bankatmlocation with locationId as the primary key (partition key).   
• Enable DynamoDB Streams for the bankatmlocation table to capture data changes.

#### Step 2: Lambda Function to Index Data in Algolia

• Implement a Lambda function that is triggered by DynamoDB Streams when new data is inserted.  
• The Lambda function extracts relevant information from the DynamoDB event, constructs a complete address, and then uses Algolia to index the data for efficient location-based search.  
• The Lambda function code can be found in the lambda_function.py file.    

#### Step 3: Configure Lambda Role and Trigger

• Grant the Lambda function the necessary permissions to access DynamoDB and the AWS Location Service.   
• Create a DynamoDB trigger to invoke the Lambda function whenever new data is inserted into the bankatmlocation table.

#### Step 4: Create Lambda Layer for Algolia

• Set up a Lambda Layer containing the algoliasearch library to be used by the Lambda function.    
• Detailed instructions for creating and uploading the Lambda Layer can be found in the lambda_layers directory.

#### Step 5: Run DynamoDB Queries to Ingest Data

• Insert sample ATM location data into the bankatmlocation DynamoDB table using SQL-like queries.  
• This step populates the DynamoDB table with sample data for testing and demonstration.  

#### Step 6: Implement Location-Based Search Lambda

•Develop another Lambda function that performs location-based search using the Algolia index.   
•This Lambda function takes a complete address as input and returns nearby ATM locations.    
•The Lambda function code for location-based search can be found in the location_search_lambda.py file.

#### Step 7: Create API Gateway

• Set up an API using AWS API Gateway to expose the location-based search functionality.  
•The API Gateway routes incoming requests to the location search Lambda function.   

#### Step 8: Perform End-to-End Testing

• Test the complete system end-to-end using sample test cases.    
•This step ensures that the location-based search service works as expected.

### Testing the Location-Based Search
To test the location-based search functionality, you can use the following test cases:

Search for ATM locations near "Kalka chownk, New Vita Enclave Rd, near vita milk plant, Ambala, Haryana 134003".  
Search for ATM locations near " Range hills, near khadki , Pune, Maharashtra 411020".  
Search for ATM locations near "House no 2, Mother teresa road, Geetanagar, Auditek, bylane, Guwahati, Assam 781020".    
Search for ATM locations near "North, 400, Grand Trunk Rd, Salkia, Howrah, West Bengal 711101".

### Installation and Setup

Follow the steps outlined in each section to set up and configure the project components, including DynamoDB, Lambda functions, Algolia, API Gateway, and testing.


Note: Replace placeholders such as {App Name}, {API Key}, {Index/Algolia Table Name}, {Place Index name}, and [Your Name] with appropriate values and details.

