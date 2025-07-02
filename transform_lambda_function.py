import json
import boto3
import pandas as pd
from datetime import datetime
import io
import os
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    S3_PROCESSED_BUCKET=os.environ['S3_PROCESSED_BUCKET']

    for record in event['Records']:
        # Get bucket and key from S3 event
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        #key = record['s3']['object']['key']
        print(bucket)
        print(key)
        print(f"{key}is extracted from s3")
        
        try:
            # Read raw data from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            print("trying to fetch the data!!")
            raw_data = json.loads(response['Body'].read())

            
            # Transform data
            transformed_data = transform_weather_data(raw_data)
            print("data is transformed")
            
            # Create processed data key
            processed_key = key.replace('weather-data-raw', 'weather-data-processed').replace('.json', '.parquet')
            
            # Convert to Parquet and upload
            df = pd.DataFrame([transformed_data])
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            
            s3_client.put_object(
                Bucket=S3_PROCESSED_BUCKET,
                Key=processed_key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"Successfully transformed and uploaded: {processed_key}")
            
        except Exception as e:
            print(f"Error processing {key}: {str(e)}")
    
    return {'statusCode': 200, 'body': 'Transformation completed'}

def transform_weather_data(raw_data):
    """Transform raw weather data into structured format"""
    return {
        'timestamp': raw_data.get('extraction_timestamp'),
        'city': raw_data.get('city_name'),
        'country': raw_data.get('sys', {}).get('country'),
        'latitude': raw_data.get('coord', {}).get('lat'),
        'longitude': raw_data.get('coord', {}).get('lon'),
        'temperature': raw_data.get('main', {}).get('temp'),
        'feels_like': raw_data.get('main', {}).get('feels_like'),
        'humidity': raw_data.get('main', {}).get('humidity'),
        'pressure': raw_data.get('main', {}).get('pressure'),
        'weather_main': raw_data.get('weather', [{}])[0].get('main'),
        'weather_description': raw_data.get('weather', [{}])[0].get('description'),
        'wind_speed': raw_data.get('wind', {}).get('speed'),
        'wind_direction': raw_data.get('wind', {}).get('deg'),
        'visibility': raw_data.get('visibility'),
        'cloudiness': raw_data.get('clouds', {}).get('all'),
        'sunrise': raw_data.get('sys', {}).get('sunrise'),
        'sunset': raw_data.get('sys', {}).get('sunset')

    }