import requests, zipfile
import os
import io

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

directory = "downloads"

def create_storage_folder():
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        print('---- Cleanning downloads folder ----\n')
        for root, dirs, files in os.walk(directory, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

def download_file_http():
    for i, uri in enumerate(download_uris):
            try:
                print(f'---- Start download file: {i+1} with uri: {uri} ----')
                r = requests.get(uri)
                with open(directory + "/" + uri.split("/")[-1], "wb") as fd:
                    for chunk in r.iter_content(chunk_size=128):
                        fd.write(chunk)
                
            except Exception as e:
                print(f'Exception when download file {i+1} - error: {e}')
    print("\n")
    

def extract_csv_file():
    for i, uri in enumerate(download_uris):
        try:
            print(f"---- Start unzip file {i+1} ---")
            file_path = directory + "/" + uri.split("/")[-1]
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(directory)
                # os.remove(file_path)
        except Exception as e:
            print(f'Exception when extract csv file {i+1} - error: {e}')
    print("\n")

    print('Remove original zip file\n')
    for uri in download_uris:
        os.remove(directory + "/" + uri.split("/")[-1])

def main():
    # 1. Create directory downloads if not exists
    create_storage_folder()
    
    # 2. Download file from uri sequentially
    download_file_http()

    # 3. Extract csv from zip file
    extract_csv_file()

def async_http():
    pass


if __name__ == "__main__":
    main()
