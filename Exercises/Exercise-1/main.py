import requests, zipfile
import os
import aiohttp
import asyncio
import aiofiles
from concurrent.futures import ThreadPoolExecutor
import time

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
    start = time.perf_counter()
    for i, uri in enumerate(download_uris):
            try:
                print(f'---- Start download file: {i+1} with uri: {uri} ----')
                r = requests.get(uri)
                with open(directory + "/" + uri.split("/")[-1], "wb") as fd:
                    for chunk in r.iter_content(chunk_size=128):
                        fd.write(chunk)
                
            except Exception as e:
                print(f'Exception when download file {i+1} - error: {e}')
    print(f'\nTotal time download with thread pool: {time.perf_counter() - start}\n')
    

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
    # extract_csv_file()


# ---- Thread pool executor with requests download ----

def download_single_file_http(uri):
    try:
        print(f'---- Start download file with uri: {uri} ----')
        r = requests.get(uri)
        with open(directory + "/" + uri.split("/")[-1], "wb") as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)            
    except Exception as e:
        print(f'Exception when download file - error: {e}')

def thread_pool_download():
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_single_file_http, download_uris)

    print(f'\nTotal time download with thread pool: {time.perf_counter() - start}\n')


def thread_pool_main():
    # 1. Create directory downloads if not exists
    create_storage_folder()

    # 2. Download file from uri sequentially
    thread_pool_download()

    # 3. Extract csv from zip file
    # extract_csv_file()

# ----- End thread pool executor download -----


# ----- Async IO download method -----
def async_main():
    create_storage_folder()

    start = time.perf_counter()
    sema = asyncio.Semaphore(5)

    async def fetch_file(session: aiohttp.ClientSession, uri):
        file_name = directory + "/" + uri.split("/")[-1]
        print(f'---- Start download file with uri: {uri} ----')
        async with sema:
            async with session.get(uri) as resp:
                data = await resp.read()
        
        async with aiofiles.open(file_name, "wb") as output_file:
            await output_file.write(data)
    
    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_file(session, uri) for uri in download_uris]
            await asyncio.gather(*tasks)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
    print(f'\nTotal time download with asyncio: {time.perf_counter() - start}\n')
# ----- End async IO download method -----


if __name__ == "__main__":
    main()
    thread_pool_main()
    async_main()
