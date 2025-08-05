from multiprocessing import Pool

from geopandas import read_file
from pandas import read_csv
from tqdm import tqdm

from ..config import PARALLEL_DOWNLOADS, cwd, data_dir
from ..download import download_csv
from ..group import group_by_adm0

DATASET_LINKS = "https://researchsites.withgoogle.com/tiles.geojson"
CSV_COLUMNS = "area_in_meters,confidence"


def partition() -> None:
    """Partition downloaded building footprints into ADM0 regions."""
    country_list = cwd / "microsoft/countries.csv"
    country_lookup = read_csv(country_list, usecols=["ISO3"]).drop_duplicates()
    country_codes = country_lookup["ISO3"].to_list()
    pbar = tqdm(country_codes)
    for iso3 in pbar:
        pbar.set_description(iso3)
        group_by_adm0("google", iso3)


def download_files(urls: list[str]) -> None:
    """Download files in parallel.

    Iterate through the URL list and passses download links to GDAL for processing.
    """
    results = []
    pool = Pool(PARALLEL_DOWNLOADS)
    for url in urls:
        input_url = f"CSV:/vsigzip//vsicurl/{url}"
        output_dir = data_dir / "google" / "inputs"
        file_name = url.split("/")[-1]
        output_path = output_dir / file_name.replace(".csv.gz", ".parquet")
        args = [input_url, output_path, CSV_COLUMNS]
        result = pool.apply_async(download_csv, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def download() -> None:
    """Read the master list of building footprint URLs and download them."""
    dataset_links = read_file(DATASET_LINKS, use_arrow=True, columns=["tile_url"])
    urls = dataset_links["tile_url"].to_list()
    download_files(urls)


def main() -> None:
    """Entrypoint to the function."""
    download()
    partition()


if __name__ == "__main__":
    main()
