from src.download_encode import construct_matrix, get_file_accessions
import os

def main():
    if not os.path.exists('accessions.pkl'):
        get_file_accessions()
    construct_matrix()

if __name__ == "__main__":
    main()
