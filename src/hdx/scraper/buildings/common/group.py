from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run

from .config import AWS_ENDPOINT_S3, HDX_MAX_SIZE
from .extract import extract_country_buildings
from .split import split_into_parts


def group(provider: str, iso3: str, output_dir: Path) -> None:
    """Create a zipped File Geodatabase for a given country."""
    input_path = f"s3://{AWS_ENDPOINT_S3}/hdx/{provider}-open-buildings/geoparquet-2.0/**/*_buildings.parquet"
    output_name = f"{iso3.lower()}_buildings"
    rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_gpq = output_dir / f"{output_name}.parquet"
    output_gdb = output_dir / f"{output_name}.gdb"
    output_gdb_zip = output_dir / f"{output_name}.gdb.zip"
    if not extract_country_buildings(iso3, input_path, output_gpq):
        return
    run(["gdal", "vector", "convert", output_gpq, output_gdb, "--quiet"], check=False)
    make_archive(str(output_gdb), "zip", output_gdb)
    rmtree(output_gdb)
    zip_size = output_gdb_zip.stat().st_size
    if zip_size > HDX_MAX_SIZE:
        output_gdb_zip.unlink()
        split_into_parts(output_dir, iso3, zip_size)
    output_gpq.unlink(missing_ok=True)
