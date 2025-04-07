# Download ENCODE file metadata into a polars dataframe

## Install project with uv
See [uv](https://docs.astral.sh/uv/getting-started/installation/) for os-specific installation instructions.

```bash
uv sync --no-dev
```

## Run example to register files
```bash
uv run main.py
```

## If you just want the files
[Dropbox link]()

## Description of columns
### Preface
Check out [encode_matrix.py](src/encode_matrix.py) to see exactly how the DataFrame is filtered and cleaned from the raw JSON. I opted to only include experiment-related and 'status' == 'released' files, as opposed to all files in the ENCODE database. However if you require more granularity and have specific requirements, filter and modify the raw JSON as you see fit.

### Descriptions
+--------------------------+----------------------------------------------------+
| DataFrame Column         | Description                                        |
+--------------------------+----------------------------------------------------+
| id                       | The title of the file either the accession or the  |
|                          | external_accession.                                |
| accession                | A unique identifier to be used to reference the    |
|                          | object prefixed with ENC.                          |
| experiments              | The experiment or dataset the file belongs to.     |
| assay_term_name          | Type of assay performed on the file.               |
| biosample                | Biosample                                          |
| organ_slims              | Organ slims                                        |
| simple_biosample_summary | Simple biosample summary                           |
| output_category          | The origin batch biosample(s) associated with this |
|                          | file.                                              |
| output_type              | A description of the file's purpose or contents.   |
| target                   | Targets of assay                                   |
| file_format              | File extension.                                    |
| file_type                | The concatenation of file_format and               |
|                          | file_format_type                                   |
| file_format_type         | Files of type bed and gff require further          |
|                          | specification                                      |
| download_link            | URL to download the file.                          |
| assembly                 | Genome assembly that files were mapped to.         |
| genome_annotation        | Genome annotation that file was generated with.    |
| biological_replicates    | The biological replicate numbers associated with   |
|                          | this file.                                         |
| technical_replicates     | The technical replicate numbers associated with    |
|                          | this file.                                         |
| index_of                 | The files this index file is relevant for.         |
| derived_from             | The files participating as inputs into software to |
|                          | produce this output file.                          |
| origin_batches           | The origin batch biosample(s) associated with this |
|                          | file.                                              |
| paired_with              | The paired end fastq that corresponds with this    |
|                          | file.                                              |
| paired_end               | Which read of the pair the file represents (in     |
|                          | case of paired end sequencing run)                 |
| read_length              | For high-throughput sequencing, the number of      |
|                          | contiguous nucleotides determined by sequencing.   |
| run_type                 | Indicates if file is part of a single or paired    |
|                          | end sequencing run                                 |
| read_length_units        | The units for read length.                         |
| mapped_read_length       | The length of the reads actually mapped, if the    |
|                          | original read length was clipped.                  |
| mapped_run_type          | The mapped run type of the alignment file which    |
|                          | may differ from the fastqs it is derived from.     |
| step_run                 | The run instance of the step used to generate the  |
|                          | file.                                              |
| preferred_default        | A flag to indicate whether this file should be     |
|                          | used by default for purposes such as downloading,  |
|                          | visualization, etc.                                |
| file_size                | File size specified in bytes.                      |
| file_size_mb             | File size specified in megabytes.                  |
| date_created             | Date created                                       |
| project                  | Project affiliation                                |
| lab                      | Lab                                                |
| software                 | Software used to generate the file.                |
+--------------------------+----------------------------------------------------+

# TODO
- [ ] MD5 checksum
- [ ] Aware (i.e. ENCODE version)
- [ ] add assay_title