#!/usr/bin/env python3
import boto3

def get_s3_size(bucket, path=''):
    """מקבל bucket ונתיב ומדפיס את הגודל ב-GB"""
    s3_client = boto3.client('s3')
    
    total_size_bytes = 0
    
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=path):
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size_bytes += obj['Size']
    
    total_size_gb = total_size_bytes / (1024 ** 3)
    print(f"גודל: {total_size_gb:.2f} GB")

if __name__ == "__main__":
    bucket = 'gepeta-datasets'
    path = 'processed_cleaned_filtered/run_5_files'
    get_s3_size(bucket, path)
