from asyncio import Semaphore, TaskGroup, run

from httpx import AsyncClient
from pandas import read_csv

from ..common.config import CONCURRENCY_LIMIT, PROVIDER_MICROSOFT, TIMEOUT, data_dir
from ..common.download import download_gz, upload_to_s3, vector_to_geoparquet

DATASET_LINKS = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)


async def _fetch_url(client: AsyncClient, url: str, semaphor: Semaphore) -> None:
    """Download a large file from a URL in chunks using httpx."""
    async with semaphor:
        output_dir = data_dir / PROVIDER_MICROSOFT / "inputs"
        file_name = url.rsplit("/global-buildings.geojsonl/", maxsplit=1)[-1]
        output_path = output_dir / file_name.replace(".csv.gz", ".geojsonl")
        output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
        sorted_parquet = output_dir / file_name.replace(".csv.gz", ".sorted.parquet")
        await download_gz(client, url, output_path)
        # Convert from raw with SORT_BY_BBOX once; other variants reuse this sorted file
        await vector_to_geoparquet(
            output_path, sorted_parquet, use_parquet_geo_types="YES", sort_by_bbox=True
        )
        await upload_to_s3(
            PROVIDER_MICROSOFT,
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
                sorted_parquet, output_parquet, use_parquet_geo_types=use_geo_types
            )
            await upload_to_s3(
                PROVIDER_MICROSOFT, output_dir, output_parquet, subfolder
            )
            output_parquet.unlink()
        sorted_parquet.unlink()
        output_path.unlink()


async def _download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    semaphore = Semaphore(CONCURRENCY_LIMIT)
    async with AsyncClient(timeout=TIMEOUT) as client, TaskGroup() as tg:
        [tg.create_task(_fetch_url(client, url, semaphore)) for url in urls]


def main() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_csv(DATASET_LINKS, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    run(_download_files(urls))


if __name__ == "__main__":
    main()
