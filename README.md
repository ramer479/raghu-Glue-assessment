# raghu-emesa-assessment
This repository has the solution by Raghuram for the assessment provided in here. https://github.com/EmesaDEV/data-engineer-assignment

## 1.Data Pipeline loading CSV file to Database 

**Architecture for Data pipeline**
<p align="left" width="100%">
  <img width="70%" src="https://user-images.githubusercontent.com/42775479/203854451-a9dabc7a-4321-4771-bbdb-fc2543e63524.jpg">
</p>

**Steps in Datapipeline**
- Upload a csv file to specific path in s3. i.e., s3://emesa-data-bucket/curz/files/transaction/
- An Event based trigger is set up to the s3 Bucket and key suffix/prefix
- This action triggers the Lambda emesa-glue-trigger
- Lambda function takes the Job name parameters from the JSON config file "config.json"
- Based on extracted job paramter, Lambda function triggers the Glue job
- Glue job picks the RDS credentials from the Secrets manager
- A pyspark script is written to pick the dataframe from particular location and insert data into Amazon RDS MySQL
- Data is now available in RDS 

## 2.Build and deploy an API-endpoint to query the data

**API Endpoint Architecture**

![Architecture_Api](https://user-images.githubusercontent.com/42775479/203854482-cd00cc29-e177-4806-837c-73757959efce.png)

https://yl41ngmlg4.execute-api.us-east-1.amazonaws.com/dev/transactions?numRecords=25
