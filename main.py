from src.encode_matrix import EncodeMatrix

def main():
    encode_matrix = EncodeMatrix(path='output', schema_type='files')
    # encode_matrix.get_size_of_encode()
    # encode_matrix.to_static_json()
    # encode_matrix.to_parquet()
    
    # Method only available for schema_type = 'files'
    encode_matrix.filter_files_parquet()

if __name__ == "__main__":
    main()
 