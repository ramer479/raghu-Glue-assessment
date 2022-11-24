import json
import boto3
import pymysql
import logging 
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
secret_client = boto3.client('secretsmanager')

def get_secret():
    """This method is to extract secrets for RDS from secretsmanager service"""
    secret = ''
    secret_name = "aurora-dev-emesa"
    region_name = "us-east-1"
    try:
        secret_response = secret_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logger.error(f"Error is recorded {e}")
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
        if 'SecretString' in secret_response:
            logger.info("The secret is available")
            secret = secret_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(secret_response['SecretBinary'])
    return secret


def get_rds_conn(user_name, password, host_name, port):
    rds_conn = pymysql.connect(
        host=host_name,
        user=user_name,
        password=password,
        port=port
        )
    logger.info("The Rds connection is established")
    return rds_conn

def lambda_handler(event, context):
    # Get number of records
    record_count = int(event['queryStringParameters']['numRecords'])
    logger.info(f"The record count input given is {record_count}")
    
    # Get RDS credentials from secret
    SECRET = get_secret()
    SECRET = json.loads(SECRET)
    RDS_USER = SECRET.get('username')
    RDS_PASSWORD = SECRET.get('password')
    RDS_HOST = SECRET.get('host')
    RDS_PORT = SECRET.get('port')
    
    # Get Rds connection and execute the cursor to fetch records from fact_transact table according to records size
    conn = get_rds_conn(RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT)
    cur = conn.cursor()
    if record_count > 0:
        cur.execute(f"select * from myproject.fact_transact limit {record_count}")
    else:
        cur.execute(f"select * from myproject.fact_transact")
    records = cur.fetchall()
    records_size = len(records)
    logger.info(f"The total record count is: {records_size}")
    
    # Get the schema of the table and extract column names
    cur.execute("DESC myproject.fact_transact")
    schema_tuple = cur.fetchall()
    schema_cols = [cd[0] for cd in schema_tuple]
    logger.info(f"The schema fetched is: {schema_cols}")
    
    output_list = []
    for rec in records:
        record_dict = {}
        for i in range(len(schema_tuple)-1):
            for j in range(len(rec)-1):
                if i == j:
                    record_dict[schema_tuple[i][0]] = rec[j]
        output_list.append(record_dict)
    
    logger.info(f"Preparing the response_object according to standard")
    response_object = {}
    response_object["isBase64Encoded"] = False
    response_object['statusCode'] = 200
    response_object['headers'] = {}
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['body'] = json.dumps(output_list)
    logger.info(f"Final response object statusCode is: {response_object['statusCode']}")
    
    return response_object
    