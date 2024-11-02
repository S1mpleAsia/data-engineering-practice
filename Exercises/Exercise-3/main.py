import boto3
import os
import requests
import gzip, io

bucket_name = 'commoncrawl'
key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
backup_uri = 'https://data.commoncrawl.org/'

def backup_s3_file():
    response = requests.get(backup_uri + key, stream=True)
    assert response.status_code == 200
    
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as file:
        first_uri_file = file.readline().decode('utf-8').strip()
        print(f'Retrieve first line file: {first_uri_file}')
        return first_uri_file
    
def process_data(path):
    response = requests.get(backup_uri + path, stream=True)
    assert response.status_code == 200

    with gzip.open(response.content, 'rt', encoding='utf-8') as file:
        for line in file:
            print(line)


def load_s3_file(access_key, secret_key):
    try:
        client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        print("Gettting S3 Object ...")
        file = client.get_object(Bucket=bucket_name, Key=key) 
        print(f"Done...., file={file}")
    except Exception as e:
        print(f'Exception when access s3 bucket: {e}')
        print('Try using backup URI...')
        return backup_s3_file()


def main():
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    path = load_s3_file(access_key, secret_key)
    process_data(path)


if __name__ == "__main__":
    main()
