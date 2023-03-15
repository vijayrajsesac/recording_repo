import json
from urllib.parse import unquote
import boto3
import datetime
from botocore.client import ClientError
from array import array
import logging

status_code = 200

# Initialize you log configuration using the base class
# Retrieve the logger instance
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # post_body = unquote(event['Records'][0]['s3']['object']['key'])
        post_body = unquote(event['detail']['object']['key'])
        logger.info(post_body.split('/'))
        logger.info('POST_BODY: ' + str(post_body))
        
        status_code = 200
        sender = 'NA'
        path = 'NA'
        filename = post_body.split('/')[5]
        if 'recordings.tsv' in filename:
            print('Yes Present')
            logger.info('--VALID FILE--')
            client = boto3.client('stepfunctions')
            # status_code = client.start_execution(
            #     stateMachineArn='arn:aws:states:us-east-1:128153543912:stateMachine:sm-sfn-sesac-d-ue1-rmd-invoke_etl',
            #     name= str(datetime.datetime.now().timestamp()) +"_"+ post_body.split('/')[2] + "_" + post_body.split('/')[3],
            #     input= json.dumps({'sender' : post_body.split('/')[4].split('=')[1], 'date' : post_body.split('/')[3], 'path' : post_body})
            # )
            sender = post_body.split('/')[4].split('=')[1]
            path = post_body
            logger.info('STATUS_CODE_OBJ: ' + str(status_code))
        else:
            print('Not Present')
            logger.info('--NOT A VALID FILE--')
    except Exception as e:
        # print ('Exception: %s', % e)
        logger.info(e)
        status_code = 500
        raise
    finally:
        return{
            'status_code': status_code,
            'sender': sender,
            'path': path,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        }
