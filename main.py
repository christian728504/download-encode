from src.encode_matrix import EncodeMatrix
from src.encode_matrix_by_accession import EMByAccession

def main():
    """
    ### EncodeMatrix (old method) ###
    
    encode_matrix = EncodeMatrix(path='output/encode_matrix', schema_type='files')
    encode_matrix.get_size_of_encode()
    encode_matrix.to_static_json()
    encode_matrix.to_parquet()
    # Method only available for schema_type = 'files'
    encode_matrix.filter_parquet()
    """
    
    ### EncodeMatrixByAccession (new method) ###

    encode_matrix = EMByAccession(path='output/encode_matrix_by_accession')
    encode_matrix.get_file_accessions()
    encode_matrix.to_json()
    encode_matrix.to_parquet()
    encode_matrix.filter_parquet()

if __name__ == "__main__":
    main()
 
