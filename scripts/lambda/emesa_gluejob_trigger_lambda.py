import json
import logging 
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
glue_client = boto3.client('glue')
sns_client = boto3.client('sns')

def trigger_glue_job(job_nm):
    response = glue_client.start_job_run(
        JobName=job_nm
        )
    return response

      
def send_sns(job_status):
    """Invokes an SNS for job Success or Failure"""
    response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:811144540482:job-sns',
        Message=job_status
        )
    return response

def lambda_handler(event, context):
    print(event)
    logger.info("Initiated by s3 Event")
    with open('config.json') as file_cfg:
        conf_variables = json.loads(file_cfg.read())
        
        job_nm = conf_variables.get("job_name")
        env = conf_variables.get("env")
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        print(f"variables are {job_nm}, {env}, {s3_bucket}, {s3_key}")
        #Send SNS for Job Scheduling
        send_sns(f"S3 Event notification received by Lambda. \nJobName is {job_nm}")
        glue_response = trigger_glue_job(job_nm)
        return glue_response
    