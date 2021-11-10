from pandas import read_csv
import typer

from pathlib import Path
from typing import Optional


def validate(
    save_dir: Optional[Path] = typer.Argument("mushroom_images/"),
    tsv: Optional[Path] = typer.Option("mushrooms.tsv.gz", "--tsv")
    ):
    data = read_csv(tsv, sep="\t", header=0)

    drop_rows = []
    for index, row in data.iterrows():
        path = Path(save_dir, row['filename'])
        if not path.is_file():
            drop_rows.append(index)
            
    data = data.drop(labels=drop_rows, axis=0)

    data.to_csv(str(tsv), index=False, compression='gzip')


if __name__ == "__main__":
    typer.run(validate)
