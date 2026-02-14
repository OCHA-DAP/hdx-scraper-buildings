import gzip
import shutil
from asyncio import create_subprocess_shell
from pathlib import Path

from httpx import AsyncClient
from tenacity import retry, stop_after_attempt, wait_fixed

from .config import ATTEMPT, AWS_ENDPOINT_S3, WAIT


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
async def download_gz(client: AsyncClient, url: str, output_path: Path) -> None:
    """Download a large file from a URL in chunks using httpx."""
    output_zip = output_path.with_suffix(output_path.suffix + ".gz")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    output_path.unlink(missing_ok=True)  # noqa: ASYNC240
    output_zip.unlink(missing_ok=True)
    with output_zip.open("wb") as f:
        async with client.stream("GET", url) as r:
            r.raise_for_status()
            async for chunk in r.aiter_bytes():
                f.write(chunk)
    with gzip.open(output_zip, "rb") as f_in, output_path.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    output_zip.unlink(missing_ok=True)


async def vector_to_geoparquet(
    input_path: Path, output_path: Path, *, use_parquet_geo_types: str | None = "YES"
) -> None:
    """Download files from url to local directory."""
    output_path.parent.mkdir(exist_ok=True, parents=True)
    cmd = [
        *["gdal", "vector", "convert"],
        *[str(input_path), str(output_path)],
        "--overwrite",
        "--quiet",
        "--lco=COMPRESSION_LEVEL=15",
        "--lco=COMPRESSION=ZSTD",
        "--lco=GEOMETRY_NAME=geometry",
    ]
    if use_parquet_geo_types:
        cmd.append(f"--lco=USE_PARQUET_GEO_TYPES={use_parquet_geo_types}")
    process = await create_subprocess_shell(" ".join(cmd))
    returncode = await process.wait()
    if returncode != 0:
        raise ValueError


async def csv_to_geoparquet(
    input_path: Path,
    output_path: Path,
    columns: str,
    *,
    use_parquet_geo_types: str | None = "YES",
) -> None:
    """Download files from url to local directory."""
    output_path.parent.mkdir(exist_ok=True, parents=True)
    open_options = [
        "--oo=AUTODETECT_TYPE=YES",
        "--oo=GEOM_POSSIBLE_NAMES=geometry",
        "--oo=KEEP_GEOM_COLUMNS=NO",
    ]
    cmd = [
        *["gdal", "vector", "pipeline", "!"],
        *["read", str(input_path), *open_options, "!"],
        *["reproject", "--src-crs=EPSG:4326", "--dst-crs=EPSG:4326", "!"],
        *["select", f"{columns},geometry", "!"],
        *["write", str(output_path)],
        "--overwrite",
        "--quiet",
        "--lco=COMPRESSION_LEVEL=15",
        "--lco=COMPRESSION=ZSTD",
        "--lco=GEOMETRY_NAME=geometry",
    ]
    if use_parquet_geo_types:
        cmd.append(f"--lco=USE_PARQUET_GEO_TYPES={use_parquet_geo_types}")
    process = await create_subprocess_shell(" ".join(cmd))
    returncode = await process.wait()
    if returncode != 0:
        raise ValueError


@retry(stop=stop_after_attempt(ATTEMPT), wait=wait_fixed(WAIT))
async def upload_to_s3(
    provider: str, output_dir: Path, output_path: Path, subfolder: str = ""
) -> None:
    """Upload a file to S3 compatible storage."""
    relative_path = output_path.relative_to(output_dir)
    prefix = f"{subfolder}/" if subfolder else ""
    cmd = [
        *["aws", "s3", "cp"],
        str(output_path),
        f"s3://{AWS_ENDPOINT_S3}/hdx/{provider}-open-buildings/{prefix}{relative_path}",
    ]
    process = await create_subprocess_shell(" ".join(cmd))
    returncode = await process.wait()
    if returncode != 0:
        raise ValueError
