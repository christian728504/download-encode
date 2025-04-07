# Download ENCODE file metadata into a polars dataframe

## To install the project dependencies you must first install uv, a python environment manager
See [here](https://docs.astral.sh/uv/getting-started/installation/) for os-specific installation instructions.

## Install dependencies
```bash
uv sync --no-dev
```

## Run example to regenerate files
```bash
uv run main.py
```

## If you just want the files
[Dropbox link](https://www.dropbox.com/scl/fo/mvsjy2yl61tk5zunnx8tf/AK77PO0gGJ7hJx_84gNiK8k?rlkey=jp2u682cq9nxuxmmlnc74ynq1&st=vkstf61y&dl=0)

## Description of columns
### Preface
Check out `filter_parquet()` method in [encode_matrix_by_accession.py](src/encode_matrix_by_accession.py) to see exactly how the DataFrame is filtered and cleaned from the raw JSON. I opted to only include `'status' == 'released'` files as this is also the default on ENCODE when you search for experiments. However if you require more granularity and have specific requirements, you can download (from Dropbox), modify, and filter the raw JSON as you see fit.

### Descriptions
These column descriptions were derived from the [ENCODE schema](https://www.encodeproject.org/profiles/file) for file objects.

| DataFrame Column         | Description                                                                                                          |
| :------------------------| :--------------------------------------------------------------------------------------------------------------------|
| id                       | The title of the file either the accession or the external_accession.                                                |
| accession                | A unique identifier to be used to reference the object prefixed with ENC.                                            |
| experiments              | The experiment or dataset the file belongs to.                                                                       |
| assay_term_name          | Assay term name.                                                                                                     |
| assay_title              | Assay title                                                                                                          |
| assay_slims              |                                                                                                                      |
| cell_slims               | Cell                                                                                                                 |
| developmental_slims      |                                                                                                                      |
| system_slims             | Systems                                                                                                              |
| classification           |                                                                                                                      |
| biosample                | Biosample                                                                                                            |
| organ_slims              | Organ slims                                                                                                          |
| simple_biosample_summary | Simple biosample summary                                                                                             |
| life_stage_age           |                                                                                                                      |
| donors                   | The donor(s) associated with this file.                                                                              |
| output_category          | The origin batch biosample(s) associated with this file.                                                             |
| output_type              | A description of the file's purpose or contents.                                                                     |
| target                   | Targets of assay                                                                                                     |
| file_format              | File extension.                                                                                                      |
| file_type                | The concatenation of file_format and file_format_type                                                                |
| file_format_type         | Files of type bed and gff require further specification                                                              |
| download_link            | URL to download the file.                                                                                            |
| assembly                 | Genome assembly that files were mapped to.                                                                           |
| genome_annotation        | Genome annotation that file was generated with.                                                                      |
| biological_replicates    | The biological replicate numbers associated with this file.                                                          |
| technical_replicates     | The technical replicate numbers associated with this file.                                                           |
| index_of                 | The files this index file is relevant for.                                                                           |
| derived_from             | The files participating as inputs into software to produce this output file.                                         |
| origin_batches           | The origin batch biosample(s) associated with this file.                                                             |
| paired_with              | The paired end fastq that corresponds with this file.                                                                |
| paired_end               | Which read of the pair the file represents (in case of paired end sequencing run)                                    |
| platform                 | The measurement device used to collect data.                                                                         |
| read_count               | Number of reads in fastq file.                                                                                       |
| read_length              | For high-throughput sequencing, the number of contiguous nucleotides determined by sequencing.                       |
| run_type                 | Indicates if file is part of a single or paired end sequencing run                                                   |
| read_length_units        | The units for read length.                                                                                           |
| mapped_read_length       | The length of the reads actually mapped, if the original read length was clipped.                                    |
| mapped_run_type          | The mapped run type of the alignment file which may differ from the fastqs it is derived from.                       |
| step_run                 | The run instance of the step used to generate the file.                                                              |
| preferred_default        | A flag to indicate whether this file should be used by default for purposes such as downloading, visualization, etc. |
| file_size                | File size specified in bytes.                                                                                        |
| file_size_mb             | File size specified in megabytes.                                                                                    |
| md5sum                   | The md5sum of the file being transferred.                                                                            |
| date_created             | Date created                                                                                                         |
| rfa                      | Project affiliation & ENCODE version.                                                                                |
| lab                      | Lab                                                                                                                  |
| software                 |                                                                                                                      |
