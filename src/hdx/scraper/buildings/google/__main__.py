from asyncio import Semaphore, TaskGroup, run

from geopandas import read_file
from httpx import AsyncClient

from ..common.config import CONCURRENCY_LIMIT, PROVIDER_GOOGLE, TIMEOUT, data_dir
from ..common.download import (
    csv_to_geoparquet,
    download_gz,
    s3_file_exists,
    upload_to_s3,
    vector_to_geoparquet,
)

DATASET_LINKS = "https://researchsites.withgoogle.com/tiles.geojson"
CSV_COLUMNS = "area_in_meters,confidence"


async def _fetch_url(client: AsyncClient, url: str, semaphor: Semaphore) -> None:
    """Download a large file from a URL in chunks using httpx."""
    async with semaphor:
        output_dir = data_dir / PROVIDER_GOOGLE / "inputs"
        file_name = url.rsplit("/", maxsplit=1)[-1]
        output_file = output_dir / file_name.replace(".csv.gz", ".csv")
        output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
        if await s3_file_exists(PROVIDER_GOOGLE, "parquet", output_parquet.name):
            return
        sorted_parquet = output_dir / file_name.replace(".csv.gz", ".sorted.parquet")
        await download_gz(client, url, output_file)
        # Convert from raw with SORT_BY_BBOX once; other variants reuse this sorted file
        await csv_to_geoparquet(
            output_file,
            sorted_parquet,
            CSV_COLUMNS,
            use_parquet_geo_types="YES",
            sort_by_bbox=True,
        )
        await upload_to_s3(
            PROVIDER_GOOGLE,
            output_dir,
            sorted_parquet,
            "geoparquet-2.0",
            s3_name=output_parquet.name,
        )
        for use_geo_types, subfolder in [
            (None, "geoparquet-1.1"),
            ("ONLY", "parquet"),
        ]:
            await vector_to_geoparquet(
                sorted_parquet,
                output_parquet,
                use_parquet_geo_types=use_geo_types,
            )
            await upload_to_s3(PROVIDER_GOOGLE, output_dir, output_parquet, subfolder)
            output_parquet.unlink()
        sorted_parquet.unlink()
        output_file.unlink()


async def _download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    semaphore = Semaphore(CONCURRENCY_LIMIT)
    async with AsyncClient(timeout=TIMEOUT) as client, TaskGroup() as tg:
        [tg.create_task(_fetch_url(client, url, semaphore)) for url in urls]


def main() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_file(DATASET_LINKS, use_arrow=True, columns=["tile_url"])
    urls = dataset_links["tile_url"].to_list()
    run(_download_files(urls))


if __name__ == "__main__":
    main()
