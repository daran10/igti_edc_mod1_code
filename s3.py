import boto3
import pandas as pd

#create client

s3_client = boto3.client('s3')

s3_client.download_file('datalake-domingos-edc',
                        'data/vgsales.csv', 
                        'data/vgvendas.csv')

df = pd.read_csv('data/vgvendas.csv')     
print(df)          

s3_client.upload_file('data/vgvendas.csv', 
                    'datalake-domingos-edc', 
                    'data/vgvendas_structured.csv',
                     )