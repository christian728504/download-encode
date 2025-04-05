from src.download_encode_matrix import construct_matrix, get_file_accessions
from src.download_encode_json import construct_json
from src.clean_encode import clean_encode
from utils import download
import os

def main():
    # if not os.path.exists('accessions.pkl'):
    #     get_file_accessions()
    # construct_matrix()
    # clean_encode()
    # construct_json()
    download("https://www.encodeproject.org/files/?format=json&limit=100", "encode.json")

if __name__ == "__main__":
    main()
 