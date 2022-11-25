# emesa-assessment by Raghuram Sistla
This repository has the solution for the assessment provided in here. https://github.com/EmesaDEV/data-engineer-assignment

## 1.Data Pipeline loading CSV file to Database 

**Architecture for Data pipeline**
<p align="left" width="100%">
  <img width="70%" src="https://user-images.githubusercontent.com/42775479/203854451-a9dabc7a-4321-4771-bbdb-fc2543e63524.jpg">
</p>

**DataPipeline Features added**
- S3
  * A CSV file Uploaded to specific path in s3. i.e., "s3://emesa-data-bucket/curz/files/transaction/" creates an an Event based trigger
  * This action triggers the Lambda "emesa_gluejob_trigger_lambda.py"
- Lambda - [git link](https://github.com/ramer479/raghu-emesa-assessment/blob/main/scripts/lambda/emesa_gluejob_trigger_lambda.py)
  * Lambda function takes the Job name parameters from the JSON config file "config.json"
  * Based on extracted job paramter, Lambda function triggers the Glue job
  * Lambda triggers an **SNS** to notify that job is being triggered
- Glue - [git link](https://github.com/ramer479/raghu-emesa-assessment/blob/main/scripts/glue/emesa_glue_transaction_job.py)
  * Glue job picks the RDS credentials from the Secrets manager
  * A pyspark script is written to pick the dataframe from particular location and insert data into Amazon RDS MySQL
  * An **SNS** is triggered with Job Status [*success/Fail*]
  * Data is now available in RDS 

**Snippets**
- Snippets are added to the : https://github.com/ramer479/raghu-emesa-assessment/tree/main/images/Datapipeline Flow

**Features I would like to Add in Data pipeline**
- *Automate this file based Glue trigger for Any File* 
- Set up an RDS for metadata , where Primary key is the s3 bucket and key. 
  * Metadata has cols such as - glue_job_name, target_database, target_table, glue_arguments. Based on RDS key, we can extract the parameters and pass on to the Glue Job
- *AWS Cloudformation template* or terraform for all resources
- Tightened Permission groups for security

## 2.Build and deploy an API-endpoint to query the data

**API Endpoint Architecture**

![Architecture_Api](https://user-images.githubusercontent.com/42775479/203854482-cd00cc29-e177-4806-837c-73757959efce.png)

**API URL** 
https://yl41ngmlg4.execute-api.us-east-1.amazonaws.com/dev/transactions?numRecords=50

**API Features added**
- API Gateway
  * An API is created with GET method
  * Setup Query string to query
  * Associate the API to Lambda in backend
- Lambda [git link](https://github.com/ramer479/raghu-emesa-assessment/blob/main/scripts/lambda/emesa_getrecords_api_lambda.py)
  * Lambda function takes parameter "numRecords" Integer value - this determines number of records you want to see
    * Example - numRecords= 10
  * Lambda picks the credentials from secret manager and connects with RDS in backend
  * Based on input value. API reponds with data
  
  https://yl41ngmlg4.execute-api.us-east-1.amazonaws.com/dev/transactions?numRecords=2
  
  ![image](https://user-images.githubusercontent.com/42775479/203927670-b2c256bf-10b7-4e90-a4b0-582fa086030b.png)


**Snippets**
- Snippets are added to the : https://github.com/ramer479/raghu-emesa-assessment/tree/main/images/API

**Features I would like to Add in API**
- Feature to select columns and Filter columns
- PUT method to insert records 
- POST method to delete/update records and get response
