End to End Weather Data Pipeline  

In this project , I've built an ETL pipeline that extracts, transforms,loads the data to a target location followed by querying and Reporting.

Extract Phase:
For the Data source I Used openweather Api(free 1000 calls/day). A lambda function in Aws calls the API and retreives the data of six Indian cities and stores them in the designated s3 bucket in the json format. 

Transform Phase:

In this step, once the source files are recived in the  bucket, an event is created and a lammda function is triggered which peforms some cleaning and data type seetings and loads them into separate target bucket in the parquet format. 

Load Phase:
In this process, a python script in the local environment is run to extract files from the processed bucket and transformed and loaded to a csv file.

Querying & Reporting:
The loaded is used to query and develop reports to extract various insights like avergage temperature per city . The Rainy hours, wind speed etc,

Note:
The lambda function which make api calls is scheduled to trigger everyhour using aws eventbridge.


