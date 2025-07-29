import logging
from multiprocessing import Pool, cpu_count

from pandas import read_csv

from .iterate import iterate
from hdx.scraper.ml_buildings.microsoft.config import COUNTRY_LOOKUP, DATASET_LINKS_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    dataset_links = read_csv(DATASET_LINKS_URL)
    country_lookup = read_csv(COUNTRY_LOOKUP)
    country_lookup = (
        country_lookup.groupby("ISO3")["RegionName"].apply(list).reset_index()
    )
    results = []
    pool = Pool(processes=cpu_count() * 2)
    for row in country_lookup.to_dict("records"):
        args = [dataset_links, row]
        result = pool.apply_async(iterate, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


if __name__ == "__main__":
    main()
