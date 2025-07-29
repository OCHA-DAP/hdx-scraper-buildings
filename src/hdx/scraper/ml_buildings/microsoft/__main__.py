from multiprocessing import Pool
from shutil import make_archive, rmtree

from duckdb import connect
from pandas import read_csv

from .config import (
    DATASET_LINKS_URL,
    GLOBAL_ADMIN0,
    PARALLEL_DOWNLOADS,
    data_dir,
)
from .utils import download_file


def merge_adm0() -> None:
    input_dir = data_dir / "microsoft" / "inputs" / "**/*.parquet"
    output_dir = data_dir / "microsoft" / "outputs"
    rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)
    with connect() as con:
        con.sql("INSTALL spatial;")
        con.sql("LOAD spatial;")
        con.sql(f"""
            COPY (
                SELECT
                    a.confidence,
                    a.height,
                    a.geometry,
                    b.iso_3
                FROM '{input_dir}' AS a
                JOIN (
                    SELECT iso_3, geometry
                    FROM '{GLOBAL_ADMIN0}'
                    WHERE iso_3 = 'AND'
                ) as b
                ON ST_Intersects(a.geometry, b.geometry)
            ) TO '{output_dir}' (
                FORMAT 'gdal',
                DRIVER 'OpenFileGDB',
                SRS 'EPSG:4326',
                GEOMETRY_TYPE 'MULTIPOLYGON',
                PARTITION_BY(iso_3)
            );
        """)
    for partition in output_dir.iterdir():
        partition_gdb = partition / "data_0.gdb"
        partition_name = partition.name.split("=")[1].lower() + "_buildings.gdb"
        make_archive(str(output_dir / partition_name), "zip", partition_gdb)
        rmtree(partition, ignore_errors=True)


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
    dataset_links = read_csv(DATASET_LINKS_URL, usecols=["Url"])
    urls = dataset_links["Url"].to_list()
    download_files(urls)
    merge_adm0()


if __name__ == "__main__":
    main()
