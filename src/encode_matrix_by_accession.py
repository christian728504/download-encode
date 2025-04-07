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
    def __init__(self,
                 path: str = str(os.getcwd()),
                 experiment_url: str = "https://www.encodeproject.org/files/{f}/?format=json",
                 query_url: str = 'https://www.encodeproject.org/search/?type=Experiment&format=json&limit=all',
                 headers: dict = {'accept': 'application/json'}):
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        self.experiment_url = experiment_url
        self.query_url = query_url
        self.headers = headers


    # Doesn't work even though it did before ... Throttling issue?
    def get_file_accessions(self):
        response = requests.get(self.query_url, headers=self.headers)
        data = response.json()
        experiment_graph = data.get('@graph')
        
        accessions = []
        for experiment in tqdm(experiment_graph, desc='Getting file accessions'):
            experiment_files = experiment.get('files')
            if not experiment_files:
                print(f"No files found for experiment!")
                sys.exit(0)
            experiment_files = [file.get('@id').split('/')[-2] for file in experiment_files]
            accessions.extend(experiment_files)
        print(f"Found {len(accessions)} files")
        
        with open(f'{self.path}/accessions.pkl', 'wb') as f:
            pickle.dump(accessions, f)
        
        return os.path.exists(f'{self.path}/accessions.pkl')


    def _fetch_file_json(self, acc):
        url = self.experiment_url.format(f=acc)
        attempts = 0
        while attempts < 10:
            try:
                response = requests.get(url, headers=self.headers)
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


    def to_json(self):
        accessions = pickle.load(open(f'{self.path}/accessions.pkl', 'rb'))
        
        list_of_json_objs = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor: # KEEP AT 32 WILL THROTTLE THE API OTHERWISE
            futures = [executor.submit(self._fetch_file_json, acc) for acc in accessions]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='Fetching files ...'):
                json_obj = future.result()
                if not json_obj:
                    print(f"Error with a JSON object!")
                    sys.exit(0)
                list_of_json_objs.append(json_obj)
                    
        with open(f'{self.path}/encode_files.json', 'w') as f:
            json.dump(list_of_json_objs, f, indent=4)
            
        print("Finished")
    
    def to_parquet(self):
        with open(f'{self.path}/encode_files.json') as f:
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
        pl.json_normalize(processed_data, max_level=0, strict=False).write_parquet(f'{self.path}/encode_files.parquet')

        print(f"Parquet file saved to {self.path}/encode_files.parquet")
    
    
    def filter_parquet(self):
        print(f"Cleaning encode_files.parquet...")
        encode_files = pl.read_parquet(f'{self.path}/encode_files.parquet')
        drop_cols = encode_files.drop("@type", "audit", "quality_metrics", "title")

        only_experiments = drop_cols.filter(pl.col("dataset").str.starts_with("/experiments"))
        filter_status_released = only_experiments.filter(pl.col("status") == "released")
        drop_status = filter_status_released.drop("status")

        bio_reps_to_list = drop_status.with_columns(pl.col("biological_replicates").str.json_decode(dtype=pl.List(pl.Int64)))
        technical_reps_to_list = bio_reps_to_list.with_columns(pl.col("technical_replicates").str.json_decode(dtype=pl.List(pl.Utf8)))
        origin_batches_to_list = technical_reps_to_list.with_columns(pl.col("origin_batches").str.json_decode(dtype=pl.List(pl.Utf8)))
        derived_from_to_list = origin_batches_to_list.with_columns(pl.col("derived_from").str.json_decode(dtype=pl.List(pl.Utf8)))

        clean_label = derived_from_to_list.with_columns(pl.col("target").str.json_path_match("$.label").alias("target"))
        clean_biosample = clean_label.with_columns(pl.col("biosample_ontology").str.json_path_match("$.term_name").alias("biosample"))
        clean_organ_slims = clean_biosample.with_columns(pl.col("biosample_ontology").str.json_path_match("$.organ_slims").str.json_decode(dtype=pl.List(pl.Utf8)).alias("organ_slims"))
        clean_cell_slims = clean_organ_slims.with_columns(pl.col("biosample_ontology").str.json_path_match("$.cell_slims").str.json_decode(dtype=pl.List(pl.Utf8)).alias("cell_slims"))
        clean_developmental_slims = clean_cell_slims.with_columns(pl.col("biosample_ontology").str.json_path_match("$.developmental_slims").str.json_decode(dtype=pl.List(pl.Utf8)).alias("developmental_slims"))
        clean_system_slims = clean_developmental_slims.with_columns(pl.col("biosample_ontology").str.json_path_match("$.system_slims").str.json_decode(dtype=pl.List(pl.Utf8)).alias("system_slims"))
        clean_classification = clean_system_slims.with_columns(pl.col("biosample_ontology").str.json_path_match("$.classification").alias("classification"))

        drop_biosample_ontology = clean_classification.drop("biosample_ontology")
        index_of_to_list = drop_biosample_ontology.with_columns(pl.col("index_of").str.json_decode(dtype=pl.List(pl.Utf8)).alias("index_of"))
        clean_award = index_of_to_list.with_columns(pl.col("award").str.json_path_match("$.project").alias("project"))
        clean_rfa = clean_award.with_columns(pl.col("award").str.json_path_match("$.rfa").alias("rfa"))
        clean_platform = clean_rfa.with_columns(pl.col("platform").str.json_path_match("$.term_name").alias("platform"))
        assay_slims = clean_platform.with_columns(pl.col("replicate").str.json_path_match("$.experiment.assay_slims").str.json_decode(dtype=pl.List(pl.Utf8)).alias("assay_slims"))
        life_stage_age = assay_slims.with_columns(pl.col("replicate").str.json_path_match("$.experiment.life_stage_age").alias("life_stage_age"))
        donors = life_stage_age.with_columns(pl.col("donors").str.json_decode(dtype=pl.List(pl.Utf8)).alias("donors"))

        clean_dataset = donors.with_columns(pl.col("dataset").str.split("/").list[2].alias("experiments"))
        drop_dataset = clean_dataset.drop("dataset")

        clean_id = drop_dataset.with_columns(pl.col("@id").str.split("/").list[2].alias("id"))
        drop_old_id = clean_id.drop("@id")

        formatted_date = drop_old_id.with_columns(pl.col("date_created").cast(pl.Datetime))

        file_size_mb = formatted_date.with_columns((pl.col("file_size") / (1024**2)).round().cast(pl.Int64).alias("file_size_mb"))

        href_to_download_link = file_size_mb.with_columns(("https://www.encodeproject.org" + pl.col("href")).alias("download_link"))
        drop_href = href_to_download_link.drop("href")

        analysis_step_version_extracted = drop_href.with_columns(pl.col("analysis_step_version").str.json_decode(infer_schema_length=None).alias("analysis_step_version"))
        software = analysis_step_version_extracted.with_columns(
            pl.col("analysis_step_version").struct.field("software_versions").list.eval(
                pl.element().struct.field("software").struct.field("name") +
                pl.lit(":") +
                pl.element().struct.field("version")).alias("software"))
        drop_analysis_step_version = software.drop("analysis_step_version")

        clean_lab = drop_analysis_step_version.with_columns(pl.col("lab").str.json_path_match("$.title").alias("lab"))
        clean_step_run = clean_lab.with_columns(pl.col("step_run").str.json_path_match("$.analysis_step_version").str.split("/").list[2].alias("step_run"))
        
        clean_encode_files = clean_step_run.select(['id',
                                            'accession',
                                            'experiments',
                                            'assay_term_name',
                                            'assay_title',
                                            'assay_slims',
                                            'cell_slims',
                                            'developmental_slims',
                                            'system_slims',
                                            'classification',
                                            'biosample',
                                            'organ_slims',
                                            'simple_biosample_summary',
                                            'life_stage_age',
                                            'donors',
                                            'output_category',
                                            'output_type',
                                            'target',
                                            'file_format',
                                            'file_type',
                                            'file_format_type',
                                            'download_link',
                                            'assembly',
                                            'genome_annotation',
                                            'biological_replicates',
                                            'technical_replicates',
                                            'index_of',
                                            'derived_from',
                                            'origin_batches',
                                            'paired_with',
                                            'paired_end',
                                            'platform',
                                            'read_count',
                                            'read_length',
                                            'run_type',
                                            'read_length_units',
                                            'mapped_read_length',
                                            'mapped_run_type',
                                            'step_run',
                                            'preferred_default',
                                            'file_size',
                                            'file_size_mb',
                                            'md5sum',
                                            'date_created',
                                            'rfa',
                                            'lab',
                                            'software']).sort("id")
        
        clean_encode_files.write_parquet(f"{self.path}/clean_encode_files.parquet")
        print(f"Cleaned data saved to clean_encode_files.parquet...")