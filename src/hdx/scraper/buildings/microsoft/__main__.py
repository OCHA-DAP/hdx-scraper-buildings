from asyncio import Semaphore, TaskGroup, run

from httpx import AsyncClient
from pandas import read_csv

from ..common.config import CONCURRENCY_LIMIT, PROVIDER_MICROSOFT, TIMEOUT, data_dir
from ..common.download import download_gz, upload_to_s3, vector_to_geoparquet

DATASET_LINKS = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)


async def fetch_url(client: AsyncClient, url: str, semaphor: Semaphore) -> None:
    """Download a large file from a URL in chunks using httpx."""
    async with semaphor:
        output_dir = data_dir / PROVIDER_MICROSOFT / "inputs"
        file_name = url.split("/global-buildings.geojsonl/")[-1]
        output_path = output_dir / file_name.replace(".csv.gz", ".geojsonl")
        output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
        await download_gz(client, url, output_path)
        await vector_to_geoparquet(output_path, output_parquet)
        output_path.unlink()
        await upload_to_s3(PROVIDER_MICROSOFT, output_dir, output_parquet)
        output_parquet.unlink()


async def download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    semaphore = Semaphore(CONCURRENCY_LIMIT)
    async with AsyncClient(timeout=TIMEOUT) as client, TaskGroup() as tg:
        [tg.create_task(fetch_url(client, url, semaphore)) for url in urls]


def main() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_csv(DATASET_LINKS, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    run(download_files(urls))


if __name__ == "__main__":
    main()
