from pandas import read_csv
from tqdm import tqdm
import typer

from functools import partial
import multiprocessing as mp
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import urlretrieve


def download_url(args):
    url, save_dir = args

    filename = Path(url).name
    save_path = Path(save_dir, filename)
    try:
        urlretrieve(url, str(save_path))
        return 1
    except URLError as e:
        download_url.queue.put(args)
    return 0


def pool_init(queue):
    download_url.queue = queue


def download(
    save_dir: Optional[Path] = typer.Argument("mushroom_images/"),
    processes: Optional[int] = typer.Option(8, "--processes", "-p"),
    tsv: Optional[Path] = typer.Option("mushrooms.tsv.gz", "--tsv")
    ):
    save_dir.mkdir(parents=True, exist_ok=True)

    data = read_csv(tsv, sep="\t", header=0)
    urls = [(url, save_dir) for url in data["url"]]
    total = len(urls)

    queue = mp.Queue()
    pool = mp.Pool(processes, pool_init, [queue])

    attempt = 1
    found = 0
    remaining = urls
    while found < total:
        found += sum(list(tqdm(
            pool.imap(download_url, remaining), 
            total=len(remaining),
            desc=F"Attempt {attempt}"
        )))

        remaining = []
        while not queue.empty():
            remaining.append(queue.get())
        
        attempt += 1


if __name__ == "__main__":
    typer.run(download)
