import json
import pandas as pd
import requests
from io import BytesIO
import logging
import urllib.parse
import boto3
import ijson
from smart_open import open

session = boto3.Session()
s3_client = boto3.client('s3',region_name='us-east-1')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_metadata(location):
    """
    Extracts metadata from the given location (URL or file path).

    Args:
        location (str): The location (URL or file path) to extract metadata from.

    Returns:
        dict: A dictionary containing the extracted metadata.
    """
    metadata = {
        'link': location,
        'file_name': None,
        'link_size_in_MB': None,
        'reporting_entity_name': None,
        'reporting_entity_type': None,
        'last_updated_on': None,
        'version': None,
        'schema': None
    }
    
    try:
        response = requests.head(location)
        content_length = response.headers.get('Content-Length') or response.headers.get('content-length')
        if content_length:
            metadata['link_size_in_MB'] = int(content_length) / (1024 * 1024)
        else:
            metadata['link_size_in_MB'] = None
    except Exception as e:
        metadata['link_size_in_MB'] = 'error'
    
    try:
        with open(location, 'rb',transport_params={'client': session.client('s3')}) as fout: 
            objects1 = ijson.items(fout, 'reporting_entity_name')
            for obj1 in objects1: 
                metadata['reporting_entity_name'] = str(obj1)
                break
    except Exception as e:
        metadata['reporting_entity_name'] = 'error'
    try:    
        with open(location, 'rb',transport_params={'client': session.client('s3')}) as fout: 
            objects2 = ijson.items(fout, 'reporting_entity_type')
            for obj2 in objects2:
                metadata['reporting_entity_type'] = str(obj2)
                break
    except Exception as e:
        metadata['reporting_entity_type'] = 'error'
    try:
        with open(location, 'rb',transport_params={'client': session.client('s3')}) as fout: 
            objects3 = ijson.items(fout, 'last_updated_on')
            for obj3 in objects3:
                metadata['last_updated_on'] = str(obj3)
                break
    except Exception as e:
        metadata['last_updated_on'] = 'error'
    try:
        with open(location, 'rb',transport_params={'client': session.client('s3')}) as fout: 
            objects4 = ijson.items(fout, 'version')
            for obj4 in objects4:
                metadata['version'] =str(obj4)
                break                
    except Exception as e:
        metadata['version'] = 'error'
    
    try:
        with open(location, 'rb') as f:
            metadata['file_name'] = f.name
            parser = ijson.parse(f)
            for n, parse_item in enumerate(parser):
                prefix, event, value = parse_item[0], parse_item[1], parse_item[2]
                if (
                    prefix == 'provider_references.item.provider_groups.item.npi' or
                    prefix == 'in_network.item.negotiated_rates.item.provider_groups.item.npi.item' or
                    prefix == 'provider_references.item.location' or
                    n > 1000000
                ):
                    if prefix == 'provider_references.item.provider_groups.item.npi':
                        metadata['schema'] = 'Type 1'
                    elif prefix == 'in_network.item.negotiated_rates.item.provider_groups.item.npi.item':
                        metadata['schema'] = 'Type 2'
                    elif prefix == 'provider_references.item.location':
                        metadata['schema'] = 'Type 3'
                    else:
                        metadata['schema'] = 'Unknown'
                    break
    except Exception as e:
        metadata['schema'] = 'error'
    return metadata


def process_csv_data(csv_data):
    """
    Processes the CSV data and extracts metadata for each link.

    Args:
        csv_data (bytes): The content of the CSV file.

    Returns:
        tuple: A tuple containing the processed DataFrame and metadata dictionary.
    """

    mrf_files_df = pd.read_csv(BytesIO(csv_data))
    month = mrf_files_df['month'][0]
    payer_name = mrf_files_df['payer_name'][0]

    mrf_metadata_dict = {'links': []}

    for index, row in mrf_files_df.iterrows():
        metadata = extract_metadata(row['location'])
        metadata['month'] = month
        metadata['payer_name'] = payer_name
        mrf_metadata_dict['links'].append(metadata)

    return mrf_metadata_dict


