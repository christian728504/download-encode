import polars as pl
import requests
from tqdm import tqdm
import pickle
import concurrent.futures
import json
import time

def fetch_file_json(url):
    attempts = 0
    while attempts < 10:
        headers = {'accept': 'application/json'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    file = response.json()
                    time.sleep(0.5)
                    return file
                except json.JSONDecodeError:
                    print(f"Error: Could not decode JSON for {url}")
            elif response.status_code == 403:
                print(f"Error: Forbidden for {url}")
                time.sleep(0.5)
                attempts += 1
            elif response.status_code == 429:
                print(f"Error: Too many requests for {url}")
                response.headers.get("Retry-After", 1)
                time.sleep(int(response.headers.get("Retry-After", 1)))
                attempts += 1
            else:
                print(f"Error: Received status {response.status_code} for {url}. No retry.")
                response.raise_for_status()
                return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error: Could not fetch {url}")
            attempts += 1
            
    print(f"Error: Could not fetch {url} after {attempts} attempts.")
    return None

def construct_json():
    accessions = pickle.load(open('accessions.pkl', 'rb'))
    urls = []
    for accession in accessions:
        urls.append(f"https://www.encodeproject.org/files/{accession}/?format=json")
    
    list_of_json_objs = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(fetch_file_json, url) for url in urls]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Fetching files ...'):
            json_obj = future.result()
            if json_obj is not None: # Handle case where file is not found
                list_of_json_objs.append(json_obj)
                
    with open('encode_files_json.json', 'w') as f:
        json.dump(list_of_json_objs, f)
                
    df = pl.DataFrame(list_of_json_objs)
    print(df.head())
    
    df.write_parquet('encode_files_json.parquet')
        
    print("Saved all of encode, again >:)")