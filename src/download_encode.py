import polars as pl
import requests
from box import Box
from tqdm import tqdm
import concurrent.futures
import os

# def construct_matrix():
    
#     url = 'https://www.encodeproject.org/search/?type=File&format=json&limit=all'
#     headers = {'accept': 'application/json'}

#     response = requests.get(url, headers=headers, stream=True)
    
#     progress_bar = tqdm(total=0, unit='B', unit_scale=True, desc=url)

#     data_bytes = b''
#     for chunk in response.iter_content(chunk_size=8192):
#         if chunk:
#             data_bytes += chunk
#             progress_bar.update(len(chunk))

#     progress_bar.close()

#     data = data_bytes.decode('utf-8')
#     data = requests.utils.json.loads(data)

#     file_graph = data.get('@graph', [])
    
#     rows = []
#     for file in tqdm.tqdm(file_graph, desc='Constructing matrix from files json objects'):
#         file = Box(file)

#         row = {
#             'title': str(file.get('title', 'NA')),
#             'accession': str(file.get('accession', 'NA')),
#             'dataset': str(file.get('dataset', 'NA')),
#             'assembly': str(file.get('assembly', 'NA')),
#             'technical_replicates': str(file.get('technical_replicates', 'NA')),
#             'biological_replicates': str(file.get('biological_replicates', 'NA')),
#             'file_format': str(file.get('file_format', 'NA')),
#             'file_type': str(file.get('file_type', 'NA')),
#             'file_format_type': str(file.get('file_format_type', 'NA')),
#             'file_size': str(file.get('file_size', 'NA')),
#             'assay_term_name': str(file.get('assay_term_name', 'NA')),
#             'term_name': str(file.get('biosample_ontology', {}).get('term_name', 'NA')),
#             'organ_slims': str(file.get('biosample_ontology', {}).get('organ_slims', 'NA')),
#             'simple_biosample_summary': str(file.get('simple_biosample_summary', 'NA')),
#             'origin_batches': str(file.get('origin_batches', 'NA')),
#             'label': str(file.get('target', {}).get('label', 'NA')),
#             'href': str(file.get('href', 'NA')),
#             'derived_from': str(file.get('derived_from', 'NA')),
#             'genome_annotation': str(file.get('genome_annotation', 'NA')),
#             'paired_end': str(file.get('paired_end', 'NA')),
#             'paired_with': str(file.get('paired_with', 'NA')),
#             'preferred_default': str(file.get('preferred_default', 'NA')),
#             'run_type': str(file.get('run_type', 'NA')),
#             'read_length': str(file.get('read_length', 'NA')),
#             'mapped_read_length': str(file.get('mapped_read_length', 'NA')),
#             'cropped_read_length': str(file.get('cropped_read_length', 'NA')),
#             'cropped_read_length_tolerance': str(file.get('cropped_read_length_tolerance', 'NA')),
#             'mapped_run_type': str(file.get('mapped_run_type', 'NA')),
#             'read_length_units': str(file.get('read_length_units', 'NA')),
#             'output_category': str(file.get('output_category', 'NA')),
#             'output_type': str(file.get('output_type', 'NA')),
#             'index_of': str(file.get('index_of', 'NA')),
#             'quality_metrics': str(file.get('quality_metrics', 'NA')),
#             'lab_title': str(file.get('lab', {}).get('title', 'NA')),
#             'project': str(file.get('award', {}).get('project', 'NA')),
#             'step_run': str(file.get('step_run', 'NA')),
#             'date_created': str(file.get('date_created', 'NA')),
#             'analysis_step_version': str(file.get('analysis_step_version', 'NA')),
#             'restricted': str(file.get('restricted', 'NA')),
#             'submitter_comment': str(file.get('submitter_comment', 'NA')),
#             'status': str(file.get('status', 'NA')),
#             'annotation_type': str(file.get('annotation_type', 'NA')),
#             'annotation_subtype': str(file.get('annotation_subtype', 'NA')),
#             'biochemical_inputs': str(file.get('biochemical_inputs', 'NA')),
#             'encyclopedia_version': str(file.get('encyclopedia_version', 'NA'))
#         }
#         rows.append(row)

    # df = pl.DataFrame(rows)
    # print(df.head())
    # df.write_parquet('encode_files.parquet')
    # print("Saved all of encode >:)")

def _fetch_file_data(file):
    file = Box(file)
    return {
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
        'href': str(file.get('href', 'NA')),
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
        'status': str(file.get('status', 'NA')),
        'annotation_type': str(file.get('annotation_type', 'NA')),
        'annotation_subtype': str(file.get('annotation_subtype', 'NA')),
        'biochemical_inputs': str(file.get('biochemical_inputs', 'NA')),
        'encyclopedia_version': str(file.get('encyclopedia_version', 'NA'))
    }

def construct_matrix():
    url = 'https://www.encodeproject.org/search/?type=File&format=json&limit=all'
    headers = {'accept': 'application/json'}

    response = requests.get(url, headers=headers)
    data = response.json()
    file_graph = data.get('@graph', [])

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(_fetch_file_data, file_graph), total=len(file_graph)))

    df = pl.DataFrame(results)
    print(df.head())
    output_path = os.path.join('..', 'encode_files.parquet')
    df.write_parquet(output_path)
    
    print("Saved all of encode >:)")