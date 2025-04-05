import polars as pl
import requests
from box import Box
from tqdm import tqdm
import pickle
import concurrent.futures
import json
import time
import sys
import os

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

def fetch_file_json(url):
    row = {}
    attempts = 0
    while attempts < 10:
        headers = {'accept': 'application/json'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    file = response.json()
                    file = Box(file)
                    row = {
                        'title': str(file.get('title', 'NA')),
                        'accession': str(file.get('accession', 'NA')),
                        'dataset': str(file.get('dataset', 'NA')),
                        'assembly': str(file.get('assembly', 'NA')),
                        'technical_replicates': str(file.get('technical_replicates', 'NA')),
                        'biological_replicates': str(file.get('biological_replicates', 'NA')),
                        'file_format': str(file.get('file_format', 'NA')),
                        'file_type': str(file.get('file_type', 'NA')),
                        'file_format_type': str(file.get('file_format_type', 'NA')),
                        'file_size': str(file.get('file_size', 'NA')),
                        'assay_term_name': str(file.get('assay_term_name', 'NA')),
                        'term_name': str(file.get('biosample_ontology', {}).get('term_name', 'NA')),
                        'organ_slims': str(file.get('biosample_ontology', {}).get('organ_slims', 'NA')),
                        'simple_biosample_summary': str(file.get('simple_biosample_summary', 'NA')),
                        'origin_batches': str(file.get('origin_batches', 'NA')),
                        'label': str(file.get('target', {}).get('label', 'NA')),
                        'download_url': "https://encodeproject.org" + str(file.get('href', 'NA')),
                        'derived_from': str(file.get('derived_from', 'NA')),
                        'genome_annotation': str(file.get('genome_annotation', 'NA')),
                        'paired_end': str(file.get('paired_end', 'NA')),
                        'paired_with': str(file.get('paired_with', 'NA')),
                        'preferred_default': str(file.get('preferred_default', 'NA')),
                        'run_type': str(file.get('run_type', 'NA')),
                        'read_length': str(file.get('read_length', 'NA')),
                        'mapped_read_length': str(file.get('mapped_read_length', 'NA')),
                        'cropped_read_length': str(file.get('cropped_read_length', 'NA')),
                        'cropped_read_length_tolerance': str(file.get('cropped_read_length_tolerance', 'NA')),
                        'mapped_run_type': str(file.get('mapped_run_type', 'NA')),
                        'read_length_units': str(file.get('read_length_units', 'NA')),
                        'output_category': str(file.get('output_category', 'NA')),
                        'output_type': str(file.get('output_type', 'NA')),
                        'index_of': str(file.get('index_of', 'NA')),
                        'quality_metrics': str(file.get('quality_metrics', 'NA')),
                        'lab_title': str(file.get('lab', {}).get('title', 'NA')),
                        'project': str(file.get('award', {}).get('project', 'NA')),
                        'step_run': str(file.get('step_run', 'NA')),
                        'date_created': str(file.get('date_created', 'NA')),
                        'analysis_step_version': str(file.get('analysis_step_version', 'NA')),
                        'restricted': str(file.get('restricted', 'NA')),
                        'submitter_comment': str(file.get('submitter_comment', 'NA')),
                        'status': str(file.get('status', 'NA'))
                    }
                    time.sleep(0.5)
                    return row
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

def construct_matrix():
    accessions = pickle.load(open('accessions.pkl', 'rb'))
    urls = []
    for accession in accessions:
        urls.append(f"https://www.encodeproject.org/files/{accession}/?format=json")
    
    rows = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(fetch_file_json, url) for url in urls]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Fetching files ...'):
            row = future.result()
            if row is not None: # Handle case where file is not found
                rows.append(row)
        
    df = pl.DataFrame(rows)
    print(df.head())
    
    df.write_parquet('encode.parquet')
    print("Saved all of encode >:)")