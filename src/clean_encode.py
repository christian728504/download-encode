
import polars as pl
import json
import os

def clean_encode():
    encode_df = pl.read_parquet('encode_files.parquet')
    encode_df = pl.DataFrame([column.replace({"NA": None}) for column in encode_df.iter_columns()])
    encode_df = encode_df.drop(pl.col("annotation_type", "annotation_subtype", "biochemical_inputs", "encyclopedia_version"))
    encode_df = encode_df.with_columns(pl.col("file_size", "read_length", "mapped_read_length", "cropped_read_length", "cropped_read_length_tolerance").cast(pl.Float64))
    encode_df = encode_df.with_columns(
        pl.col("preferred_default", "restricted").map_elements(
            lambda x: True if x.lower() in ["true"] else 
                    False if x.lower() in ["false"] else None, return_dtype=pl.Boolean)
    )
    encode_df = encode_df.with_columns(pl.col("date_created").cast(pl.Datetime))
    encode_df = encode_df.with_columns(pl.col("technical_replicates", "biological_replicates", "organ_slims", "origin_batches", "derived_from").map_elements(lambda x: json.loads(x.replace("'", '"'))))
    encode_df = encode_df.drop(pl.col("quality_metrics", "step_run", "analysis_step_version")) # json not playing nice with parser, opting to drop it
    clean_encode = encode_df

    clean_encode.write_parquet('clean_encode_files.parquet')
    
    return os.path.exists('clean_encode_files.parquet')