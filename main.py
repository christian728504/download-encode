from src.download_encode_matrix import construct_matrix, get_file_accessions
from src.download_encode_json import construct_json
from src.clean_encode import clean_encode
import os

def main():
    if not os.path.exists('accessions.pkl'):
        get_file_accessions()
    # construct_matrix()
    # clean_encode()
    construct_json()
    

if __name__ == "__main__":
    main()
