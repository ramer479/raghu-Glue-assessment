import sys
import json
import pymysql
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DateType
from pyspark.sql import functions as F

import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME'
])

# ENVRN = args['env']
# S3_BUCKET_NAME = args['s3_bucket']
# S3_KEY = args['key']

sc = SparkContext()
sc.setLogLevel("ERROR")
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("EmesaScenario").getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sns_client = boto3.client("sns")


def get_secret():
    """This method is to extract secrets for RDS from secretsmanager service"""
    secret = ''
    #secret_name = f"aurora-{ENVRN}-emesa"
    secret_name = "aurora-dev-emesa"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret


def get_rds_conn(user_name, password, host_name, port):
    """This method is to establish a RDS connection with necessary parameters. This method returns RDS connection"""
    rds_conn = pymysql.connect(
        host=host_name,
        user=user_name,
        password=password,
        port=port
        )
    return rds_conn
    
def import_data():
    """This method takes data and applies schema on top of it and returns a Dataframe"""
    data_path = "s3://emesa-data-bucket/curz/data/sample.csv"
    data_schema = StructType([ \
        StructField("order_id",LongType(),False), \
        StructField("source",StringType(),True), \
        StructField("customer_id",LongType(),False), \
        StructField("payment_id",LongType(),True), \
        StructField("voucher_id",LongType(),True), \
        StructField("product_id",LongType(),True), \
        StructField("website",StringType(),True), \
        StructField("order_status", StringType(), True), \
        StructField("voucher_status", StringType(), True), \
        StructField("payment_status", StringType(), True), \
        StructField("order_date", DateType(), True), \
        StructField("payment_date", DateType(), True) \
        ])
    
    df = spark.read.format("csv")\
                   .option("header","True")\
                   .option("schema",data_schema)\
                   .load(data_path)
    df.show(truncate=False)
    return df 

def export_data(df,user_name, password, host_name, port):
    """This method establishes a JDBC connection to RDS and Writes the source data to target table in Aurora mysql"""
    url = "jdbc:mysql://"+host_name+"/"+"myproject"
    df_transform = df.withColumn("load_dtm",F.current_timestamp())
    df_transform.write.format('jdbc').options(
      url=url,
      driver='com.mysql.jdbc.Driver',
      dbtable='fact_transact',
      user=user_name,
      password=password).mode('overwrite').save()
      
      
def send_sns(job_status):
    """Invokes an SNS for job Success or Failure"""
    response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:811144540482:job-sns',
        Message=job_status
        )
    return response
    
class DataLoadException(Exception):
    pass
    
if __name__ == "__main__":
    SECRET = get_secret()
    SECRET = json.loads(SECRET)
    RDS_USER = SECRET.get('username')
    RDS_PASSWORD = SECRET.get('password')
    RDS_HOST = SECRET.get('host')
    RDS_PORT = SECRET.get('port')
    
    print (f"DEBUG# RDS_USER = {RDS_USER} \n RDS_HOST = {RDS_HOST}")
    
    # test Block to see the connection established ###########################
    conn = get_rds_conn(RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT)
    cur = conn.cursor()
    cur.execute("SELECT VERSION()")
    version = cur.fetchone()
    print("Database version: {} ".format(version[0]))
    # test Block to see the connection established ###########################
    
    # Calling import data method for getting source dataframe from CSV 
    source_df = import_data()
    # Calling export data method to write data to RDS 
    try:
        export_data(source_df, RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT)
    except:
        send_sns("JOB HAS FAILED")
        raise DataLoadException("DATA has not been Loaded to RDS. Please fix the issue and retry")
    else:
        send_sns("JOB HAS SUCCEEDED")
    
    job.commit()
