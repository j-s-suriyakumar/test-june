import json
import logging
import urllib.parse
import boto3
from helper import process_csv_data

session = boto3.Session()
s3_client = boto3.client('s3',region_name='us-east-1')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    The entry point for the AWS Lambda function.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The runtime information provided by Lambda.

    Returns:
        dict: The response data returned by the Lambda function.
    """

    logger.info("Lambda function invoked.")

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    if file_name.endswith('.csv'):
        csv_data = response['Body'].read()

    mrf_metadata_dict = process_csv_data(csv_data)
    # Do something with mrf_files_df and mrf_metadata_dict
    print(mrf_metadata_dict)
    logger.info("Processing completed successfully.")
    

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Processing completed successfully'})
    }

