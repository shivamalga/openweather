import json
import boto3
import requests
from datetime import datetime
import os

def lambda_handler(event, context):
    # Configuration
    API_KEY = os.environ['API_KEY']
    CITIES = ['Hyderabad', 'Delhi', 'Bengaluru', 'Mumbai', 'Kolkata','Chennai']  # Add more cities
    S3_BUCKET = os.environ['S3_RAW_BUCKET']
    
    s3_client = boto3.client('s3')
    
    for city in CITIES:
        try:
            # Fetch weather data
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            weather_data = response.json()
            
            # Add timestamp and city info
            weather_data['extraction_timestamp'] = datetime.utcnow().isoformat()
            weather_data['city_name'] = city
            
            # Create S3 key with partitioning
            now = datetime.utcnow()
            s3_key = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{city}_{now.strftime('%Y%m%d_%H%M%S')}.json"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            
            print(f"Successfully uploaded data for {city}")
            
        except Exception as e:
            print(f"Error processing {city}: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Weather data extraction completed')
    }