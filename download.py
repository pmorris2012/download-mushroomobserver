
from pandas import read_csv
from tqdm import tqdm
import typer

from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import urlretrieve


def download_url(url: str, save_dir: Path):
    filename = Path(url).name
    save_path = Path(save_dir, filename)
    try:
        urlretrieve(url, str(save_path))
    except URLError as e:
        print(filename, e.reason)


def download(
    save_dir: Optional[Path] = typer.Argument("mushroom_images/"),
    processes: Optional[int] = typer.Option(8, "--processes", "-p"),
    tsv: Optional[Path] = typer.Option("mushrooms.tsv.gz", "--tsv")
    ):
    save_dir.mkdir(parents=True, exist_ok=True)

    data = read_csv(tsv, sep="\t", header=0)
    urls = data["url"].tolist()

    pool = Pool(processes)
    function = partial(download_url, save_dir=save_dir)
    result = list(tqdm(pool.imap(function, urls), total=len(urls)))


if __name__ == "__main__":
    typer.run(download)
