from pathlib import Path
from subprocess import run

# Once GDAL 3.12 is available, the following options should be added.
# --lco=COMPRESSION_LEVEL=15
# --lco=USE_PARQUET_GEO_TYPES=YES


def download_file(input_url: str, output_path: Path) -> None:
    """Download files from url to local directory."""
    output_path.parent.mkdir(exist_ok=True, parents=True)
    run(
        [
            *["gdal", "vector", "convert"],
            *[input_url, output_path],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
            "--lco=GEOMETRY_NAME=geometry",
        ],
        check=False,
    )


def download_csv(input_url: str, output_path: Path, columns: str) -> None:
    """Download files from url to local directory."""
    output_path.parent.mkdir(exist_ok=True, parents=True)
    open_options = [
        "--oo=AUTODETECT_TYPE=YES",
        "--oo=GEOM_POSSIBLE_NAMES=geometry",
        "--oo=KEEP_GEOM_COLUMNS=NO",
    ]
    run(
        [
            *["gdal", "vector", "pipeline", "!"],
            *["read", input_url, *open_options, "!"],
            *["reproject", "--src-crs=EPSG:4326", "--dst-crs=EPSG:4326", "!"],
            *["select", f"{columns},geometry", "!"],
            *["write", output_path],
            "--overwrite",
            "--lco=COMPRESSION=ZSTD",
            "--lco=GEOMETRY_NAME=geometry",
        ],
        check=False,
    )
