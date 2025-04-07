import requests
from tqdm import tqdm
import time

# modified from https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
def download(url: str, fname: str, chunk_size=1024):
    resp = requests.get(url, stream=True)
    with open(fname, 'wb') as file, tqdm(
        desc=fname,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)
            
def get_date_string():
    today = time.strftime("%d%m%Y_%H%M%S")
    return today