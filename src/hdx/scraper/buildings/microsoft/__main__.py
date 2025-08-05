import asyncio

from httpx import AsyncClient
from pandas import read_csv
from tqdm.asyncio import tqdm_asyncio

from ..common.config import SKIP_DOWNLOAD, data_dir
from ..common.download import download_gz, vector_to_geoparquet
from ..common.group import group

PROVIDER = "microsoft"
DATASET_LINKS = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)


async def fetch_url(client: AsyncClient, url: str) -> None:
    """Download a large file from a URL in chunks using httpx."""
    output_dir = data_dir / PROVIDER / "inputs"
    file_name = url.split("/global-buildings.geojsonl/")[-1]
    output_path = output_dir / file_name.replace(".csv.gz", ".geojsonl")
    output_parquet = output_dir / file_name.replace(".csv.gz", ".parquet")
    await download_gz(client, url, output_path)
    await vector_to_geoparquet(output_path, output_parquet)
    output_path.unlink()


async def download_files(urls: list[str]) -> None:
    """Download files asynchronusly."""
    async with AsyncClient(http2=True) as client:
        tasks = [fetch_url(client, url) for url in urls]
        await tqdm_asyncio.gather(*tasks)


def download() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_csv(DATASET_LINKS, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    asyncio.run(download_files(urls))


def main() -> None:
    """Entrypoint to the function."""
    if not SKIP_DOWNLOAD:
        download()
    group(PROVIDER)


if __name__ == "__main__":
    main()
