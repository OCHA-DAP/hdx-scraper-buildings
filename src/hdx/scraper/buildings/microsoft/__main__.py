from multiprocessing import Pool

from anyio import Path
from pandas import read_csv

from ..config import PARALLEL_DOWNLOADS, data_dir
from ..download import download_file
from ..group import group_by_adm0

DATASET_LINKS = (
    "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
)
COUNTRY_LOOKUP = Path(__file__).parent / "country_lookup.csv"


def download_files(urls: list[str]) -> None:
    results = []
    pool = Pool(PARALLEL_DOWNLOADS)
    for url in urls:
        input_url = f"GeoJSONSeq:/vsigzip//vsicurl/{url}"
        file_name = url.split("/global-buildings.geojsonl/")[-1]
        output_path = (
            data_dir / "microsoft" / "inputs" / file_name.replace(".csv.gz", ".parquet")
        )
        result = pool.apply_async(download_file, args=[input_url, output_path])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def download() -> None:
    dataset_links = read_csv(DATASET_LINKS, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    download_files(urls)


def partition() -> None:
    country_lookup = read_csv(COUNTRY_LOOKUP, usecols=["ISO3"]).drop_duplicates()
    country_codes = country_lookup["ISO3"].to_list()
    # for iso3 in country_codes:
    for iso3 in country_codes[0:2]:
        group_by_adm0("microsoft", iso3)


def main() -> None:
    # download()
    partition()


if __name__ == "__main__":
    main()
