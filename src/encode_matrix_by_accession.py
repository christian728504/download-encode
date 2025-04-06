import polars as pl
import requests
from tqdm import tqdm
import pickle
import concurrent.futures
import json
import time
import sys
import os

class EMByAccession:
    def __init__(self):
        pass

    def get_file_accessions():
        url = 'https://www.encodeproject.org/search/?type=Experiment&format=json&limit=all'
        headers = {'accept': 'application/json'}
        
        response = requests.get(url, headers=headers)
        data = response.json()
        experiment_graph = data.get('@graph')
        
        accessions = []
        for experiment in tqdm(experiment_graph, desc='Getting file accessions'):
            experiment_files = experiment.get('files')
            if not experiment_files:
                print(f"No files found for experiment!")
                sys.exit(0)
            experiment_files = [file.get('@id').split('/')[-2] for file in experiment_files] ## FUCK
            accessions.extend(experiment_files)
        print(f"Found {len(accessions)} files")
        
        with open('accessions.pkl', 'wb') as f:
            pickle.dump(accessions, f)
        
        return os.path.exists('accessions.pkl')


    def _fetch_file_json(url):
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
            futures = [executor.submit(_fetch_file_json, url) for url in urls]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Fetching files ...'):
                json_obj = future.result()
                if json_obj is not None:
                    list_of_json_objs.append(json_obj)
                    
        with open('encode.json', 'w') as f:
            json.dump(list_of_json_objs, f, indent=4)
            
        print("Finished")
    
    def to_parquet(self):
        with open(f'{self.path}/encode_{self.schema_type}.json') as f:
            data = json.load(f)

        processed_data = []
        for item in data:
            processed_item = {}
            for key, value in item.items():
                if isinstance(value, (dict, list, tuple, set)):
                    processed_item[key] = json.dumps(value)
                else:
                    processed_item[key] = value
            processed_data.append(processed_item)
            
        print(f"Converting JSON to polars DataFrame and saving to parquet file...")
        pl.json_normalize(processed_data, max_level=0, strict=False).write_parquet(f'{self.path}/encode_{self.schema_type}.parquet')

        print(f"Parquet file saved to {self.path}/encode_{self.schema_type}.parquet")