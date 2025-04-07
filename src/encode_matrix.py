import requests
import polars as pl
import json
import os
from enum import Enum, auto

class SchemaType(Enum):
    EXPERIMENTS = 'experiments'
    FILES = 'files'


class EncodeMatrix:
    def __init__(self,
                 schema_type: SchemaType,
                 path: str = str(os.getcwd()),
                 format_url: str = "https://www.encodeproject.org/{f}/?format=json&limit=all",
                 headers: dict = {'accept': 'application/json'}):
        self.schema_type = schema_type
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        self.format_url = format_url
        self.headers = headers
        

    def get_size_of_encode(self):
        url = self.format_url.format(f=SchemaType.EXPERIMENTS.value)
        headers = {'accept': 'application/json'}
        
        print(f"Requesting JSON from ENCODE...")
        response = requests.get(url, headers=headers)
        
        print(f"Parsing JSON from ENCODE...")
        data = response.json()
        experiment_graph = data.get('@graph', [])
        number_of_experiments = len(experiment_graph)
        number_of_files = 0
        
        print(f"Calculating total number of experiments and files on ENCODE...")
        for experiment in experiment_graph:
            experiment_files = experiment.get('files', [])
            experiment_file_count = len(experiment_files)
            number_of_files += experiment_file_count
        
        print(f"Total number of experiments on ENCODE: {number_of_experiments}")
        print(f"Total number of experiment-related files on ENCODE: {number_of_files}")
    

    def to_json(self):
        try:
            url = self.format_url.format(f=self.schema_type)
        except:
            raise TypeError(f"Invalid schema, must be one of {[e.value for e in SchemaType]}")
        headers = {'accept': 'application/json'}
        
        print(f"Requesting {self.schema_type} from ENCODE...")
        response = requests.get(url, headers=headers)
        
        print(f"Parsing {self.schema_type} JSON from ENCODE...")
        data = response.json()
        
        print(f"Dumping {self.schema_type} to JSON file...")
        file_graph = data.get('@graph', [])
        
        with open(f'{self.path}/encode_{self.schema_type}.json', 'w') as f:
            json.dump(file_graph, f, indent=4)
            
        print(f"JSON file saved to {self.path}/encode_{self.schema_type}.json")
        
        
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
        
    
    def filter_parquet(self):
        print(f"Cleaning encode_{SchemaType.FILES.value}.parquet...")
        encode_files = pl.read_parquet(f'{self.path}/encode_{SchemaType.FILES.value}.parquet')
        drop_cols = encode_files.drop("@type", "audit", "quality_metrics", "replicate", "title")

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
        drop_biosample_ontology = clean_organ_slims.drop("biosample_ontology")
        index_of_to_list = drop_biosample_ontology.with_columns(pl.col("index_of").str.json_decode(dtype=pl.List(pl.Utf8)).alias("index_of"))
        clean_award = index_of_to_list.with_columns(pl.col("award").str.json_path_match("$.project").alias("project"))

        clean_dataset = clean_award.with_columns(pl.col("dataset").str.split("/").list[2].alias("experiments"))
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
                                                    'biosample',
                                                    'organ_slims',
                                                    'simple_biosample_summary',
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
                                                    'read_length',
                                                    'run_type',
                                                    'read_length_units',
                                                    'mapped_read_length',
                                                    'mapped_run_type',
                                                    'step_run',
                                                    'preferred_default',
                                                    'file_size',
                                                    'file_size_mb',
                                                    'date_created',
                                                    'project',
                                                    'lab',
                                                    'software']).sort("id")
        
        clean_encode_files.write_parquet(f"{self.path}/clean_encode_{SchemaType.FILES.value}.parquet")
        print(f"Cleaned data saved to clean_encode_{SchemaType.FILES.value}.parquet...")