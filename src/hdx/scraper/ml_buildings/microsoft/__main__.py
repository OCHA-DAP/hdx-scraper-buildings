from multiprocessing import Pool
from time import time

from duckdb import connect
from pandas import read_csv

from .config import (
    COUNTRY_LOOKUP,
    DATASET_LINKS_URL,
    GLOBAL_ADMIN0,
    PARALLEL_DOWNLOADS,
    data_dir,
)
from .utils import download_file


def merge_adm0(iso3: str) -> None:
    input_dir = data_dir / "microsoft" / "inputs" / "**/*.parquet"
    output_file = (
        data_dir / "microsoft" / "outputs" / f"{iso3.lower()}_buildings.parquet"
    )
    output_file.parent.mkdir(exist_ok=True, parents=True)
    output_file.unlink(missing_ok=True)
    start = time()
    with connect() as con:
        con.sql(f"""
            INSTALL spatial;
            LOAD spatial;
            CREATE TABLE bounds AS (
                SELECT geometry, bbox
                FROM '{GLOBAL_ADMIN0}'
                WHERE iso_3 = '{iso3}'
            );
            SET variable xmin = (SELECT bbox.xmin FROM bounds);
            SET variable ymin = (SELECT bbox.ymin FROM bounds);
            SET variable xmax = (SELECT bbox.xmax FROM bounds);
            SET variable ymax = (SELECT bbox.ymax FROM bounds);
            SET variable boundary = (SELECT geometry FROM bounds);
            COPY (
                SELECT *
                FROM '{input_dir}'
                WHERE
                    bbox.xmin > getvariable('xmin') AND
                    bbox.xmax < getvariable('xmax') AND
                    bbox.ymin > getvariable('ymin') AND
                    bbox.ymax < getvariable('ymax') AND
                    ST_Intersects(getvariable('boundary'), geometry)
            )
            TO '{output_file}'
            WITH (COMPRESSION zstd);
        """)
        print(time() - start)


def download_files(urls: list[str]) -> None:
    results = []
    pool = Pool(PARALLEL_DOWNLOADS)
    for url in urls:
        result = pool.apply_async(download_file, args=[url])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def main() -> None:
    if False:
        dataset_links = read_csv(DATASET_LINKS_URL, usecols=["Url"])
        urls = dataset_links["Url"].to_list()
        download_files(urls)
    country_lookup = read_csv(COUNTRY_LOOKUP, usecols=["ISO3"]).drop_duplicates()
    country_codes = country_lookup["ISO3"].to_list()
    for iso3 in country_codes[0:1]:
        merge_adm0(iso3)


if __name__ == "__main__":
    main()
