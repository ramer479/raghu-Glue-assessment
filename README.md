# raghu-emesa-assessment
This repository has the solution by Raghuram for the assessment provided in here. https://github.com/EmesaDEV/data-engineer-assignment

## 1.Data Pipeline loading CSV file to Database 

**Architecture for Data pipeline**
<p align="left" width="100%">
  <img width="70%" src="https://user-images.githubusercontent.com/42775479/203854451-a9dabc7a-4321-4771-bbdb-fc2543e63524.jpg">
</p>

**DataPipeline Features added**
- S3
  * A CSV file Uploaded to specific path in s3. i.e., "s3://emesa-data-bucket/curz/files/transaction/" creates an an Event based trigger
  * This action triggers the Lambda emesa-glue-trigger
- Lambda 
  * Lambda function takes the Job name parameters from the JSON config file "config.json"
  * Based on extracted job paramter, Lambda function triggers the Glue job
  * Lambda triggers an **SNS** to notify that job is being triggered
- Glue
  * Glue job picks the RDS credentials from the Secrets manager
  * A pyspark script is written to pick the dataframe from particular location and insert data into Amazon RDS MySQL
  * An **SNS** is triggered with Job Status [*success/Fail*]
  * Data is now available in RDS 

**Snippets**
- Snippets are added to the : https://github.com/ramer479/raghu-emesa-assessment/tree/main/images/Datapipeline Flow

## 2.Build and deploy an API-endpoint to query the data

**API Endpoint Architecture**

![Architecture_Api](https://user-images.githubusercontent.com/42775479/203854482-cd00cc29-e177-4806-837c-73757959efce.png)

**API URL** 
https://yl41ngmlg4.execute-api.us-east-1.amazonaws.com/dev/transactions?select=*&numRecords=50

**API Features added**
- API Gateway
  * An API is created with GET method
  * Setup Query string to query
  * Associate the API to Lambda in backend
- Lambda 
  * Lambda function takes parameter "numRecords" Integer value - this determines number of records you want to see
    * Example - numRecords= 10
  * Lambda function takes parameter "select" String value - this determines columns you want to query
    * Example - select=order_id,source
  * Based on input value. API reponds with data


**Snippets**
- Snippets are added to the : https://github.com/ramer479/raghu-emesa-assessment/tree/main/images/Datapipeline Flow
