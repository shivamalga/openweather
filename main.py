import boto3
import os 
from dotenv import load_dotenv 
import pandas as pd 
from io import BytesIO

load_dotenv()
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
    region_name=os.getenv('REGION_NAME')
)

bucket_name = os.getenv("S3_BUCKET")

# List all objects in the bucket
response = s3.list_objects_v2(Bucket=bucket_name)

# Loop through each file and process
master_df=pd.DataFrame(columns=['timestamp','city','country','latitude','longitude','temperature','feels_like','humidity','pressure','weather_main','weather_description','wind_speed','wind_direction',
'visibility','cloudiness'])
for obj in response.get('Contents', []):
    key = obj['Key']
    print(f"Processing file: {key}")
    
    # Read file content directly
    file_obj = s3.get_object(Bucket=bucket_name, Key=key)
    content = file_obj['Body'].read()
    buffer=BytesIO(content)
    df=pd.read_parquet(buffer,engine='pyarrow')
    filtered_df=df.drop(['sunrise','sunset'],axis=1)
    filtered_df = filtered_df.astype({'wind_direction': 'float64', 'visibility': 'float64', 'cloudiness': 'float64'})
    filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'], errors='coerce')
    master_df=pd.concat([master_df,filtered_df],ignore_index=True) 


master_df.to_csv('weatherdatafile.csv',index=False)
print("File is saved !!")
    
'''follow up work :
1. handle redundant data loading 
2. wrap the code into try catch finally blocks , implement error handling '''





   
    