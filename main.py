import boto3
import os 
from dotenv import load_dotenv 
import pandas as pd 
from io import BytesIO
from datetime import datetime
cnt=0
def is_processed_file(filename):
    s=filename.split("_")
    date_str=s[1]
    previous=datetime.strptime(date_str,"%Y%m%d")
    current=datetime.now()
    diff=(current-previous).days
    if(diff==1):
        return True 
    else:
        return False 

load_dotenv()
try:
    s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
    region_name=os.getenv('REGION_NAME')
)
except Exception as e:
    print(f"unexpected error occured{e}")

bucket_name = os.getenv("S3_BUCKET")

# List all objects in the bucket
response = s3.list_objects_v2(Bucket=bucket_name)

master_file='weatherdata.csv'
if os.path.exists(master_file):
    master_df=pd.read_csv(master_file)
else:
    master_df=pd.DataFrame()

for obj in response.get('Contents', []):
    key = obj['Key']
    if(not is_processed_file(key)):
        continue

    print(f"Processing file: {key}")
    
    
    # Read file content directly
    file_obj = s3.get_object(Bucket=bucket_name, Key=key)
    content = file_obj['Body'].read()
    buffer=BytesIO(content)
    df=pd.read_parquet(buffer,engine='pyarrow')
    filtered_df=df.drop(['sunrise','sunset'],axis=1)
    filtered_df = filtered_df.astype({'wind_direction': 'float64', 'visibility': 'float64', 'cloudiness': 'float64'})
    filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp'], errors='coerce')
    filtered_df.to_csv(master_file, mode='a', header=not os.path.exists(master_file), index=False)
    cnt+=1

print("data is loaded into the master file")
print(f"{cnt} files are processed!!")











    