from asyncio import Semaphore, TaskGroup, run

from geopandas import read_file
from httpx import AsyncClient

from ..common.config import CONCURRENCY_LIMIT, PROVIDER_GOOGLE, TIMEOUT, data_dir
from ..common.download import csv_to_geoparquet, download_gz, upload_to_s3

DATASET_LINKS = "https://researchsites.withgoogle.com/tiles.geojson"
CSV_COLUMNS = "area_in_meters,confidence"


async def fetch_url(client: AsyncClient, url: str, semaphor: Semaphore) -> None:
    """Download a large file from a URL in chunks using httpx."""
    async with semaphor:
        output_dir = data_dir / PROVIDER_GOOGLE / "inputs"
        file_name = url.split("/")[-1]
        output_file = output_dir / file_name.replace(".csv.gz", ".csv")
        output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
        await download_gz(client, url, output_file)
        await csv_to_geoparquet(output_file, output_parquet, CSV_COLUMNS)
        output_file.unlink()
        await upload_to_s3(PROVIDER_GOOGLE, output_dir, output_parquet)
        output_parquet.unlink()


async def download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    semaphore = Semaphore(CONCURRENCY_LIMIT)
    async with AsyncClient(timeout=TIMEOUT) as client, TaskGroup() as tg:
        [tg.create_task(fetch_url(client, url, semaphore)) for url in urls]


def main() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_file(DATASET_LINKS, use_arrow=True, columns=["tile_url"])
    urls = dataset_links["tile_url"].to_list()
    run(download_files(urls))


if __name__ == "__main__":
    main()
