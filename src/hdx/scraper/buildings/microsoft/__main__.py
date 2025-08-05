from multiprocessing import Pool

from pandas import read_csv
from tqdm import tqdm

from ..config import PARALLEL_DOWNLOADS, cwd, data_dir
from ..download import download_file
from ..group import group_by_adm0

DATASET_LINKS = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)


def partition() -> None:
    """Partition downloaded building footprints into ADM0 regions."""
    country_list = cwd / "microsoft/countries.csv"
    country_lookup = read_csv(country_list, usecols=["ISO3"]).drop_duplicates()
    country_codes = country_lookup["ISO3"].to_list()
    pbar = tqdm(country_codes)
    for iso3 in pbar:
        pbar.set_description(iso3)
        group_by_adm0("microsoft", iso3)


def download_files(urls: list[str]) -> None:
    """Download files in parallel.

    Iterate through the URL list and passses download links to GDAL for processing.
    """
    results = []
    pool = Pool(PARALLEL_DOWNLOADS)
    for url in urls:
        input_url = f"GeoJSONSeq:/vsigzip//vsicurl/{url}"
        output_dir = data_dir / "microsoft" / "inputs"
        file_name = url.split("/global-buildings.geojsonl/")[-1]
        output_path = output_dir / file_name.replace(".csv.gz", ".parquet")
        result = pool.apply_async(download_file, args=[input_url, output_path])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def download() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_csv(DATASET_LINKS, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    download_files(urls)


def main() -> None:
    """Entrypoint to the function."""
    download()
    partition()


if __name__ == "__main__":
    main()
