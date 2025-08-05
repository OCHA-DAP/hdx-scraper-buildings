import asyncio

from geopandas import read_file
from httpx import AsyncClient
from tqdm.asyncio import tqdm_asyncio

from ..common.config import SKIP_DOWNLOAD, data_dir
from ..common.download import csv_to_geoparquet, download_gz
from ..common.group import group

PROVIDER = "google"
DATASET_LINKS = "https://researchsites.withgoogle.com/tiles.geojson"
CSV_COLUMNS = "area_in_meters,confidence"


async def fetch_url(client: AsyncClient, url: str) -> None:
    """Download a large file from a URL in chunks using httpx."""
    output_dir = data_dir / PROVIDER / "inputs"
    file_name = url.split("/")[-1]
    output_file = output_dir / file_name.replace(".csv.gz", ".csv")
    output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
    await download_gz(client, url, output_file)
    await csv_to_geoparquet(output_file, output_parquet, CSV_COLUMNS)
    output_file.unlink()


async def download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    async with AsyncClient(http2=True) as client:
        tasks = [fetch_url(client, url) for url in urls]
        await tqdm_asyncio.gather(*tasks)


def download() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_file(DATASET_LINKS, use_arrow=True, columns=["tile_url"])
    urls = dataset_links["tile_url"].to_list()
    asyncio.run(download_files(urls))


def main() -> None:
    """Entrypoint to the function."""
    if not SKIP_DOWNLOAD:
        download()
    group(PROVIDER)


if __name__ == "__main__":
    main()
