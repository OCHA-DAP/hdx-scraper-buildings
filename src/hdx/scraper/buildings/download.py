from pathlib import Path
from subprocess import run


def download_file(input_url: str, output_path: Path) -> None:
    """Download files from url to local directory.

    Once GDAL 3.12 is available, the following options should be added.
    --lco=COMPRESSION_LEVEL=15
    --lco=USE_PARQUET_GEO_TYPES=YES
    """
    output_path.parent.mkdir(exist_ok=True, parents=True)
    run(
        [
            *["gdal", "vector", "convert"],
            *[input_url, output_path],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
        ],
        check=False,
    )
