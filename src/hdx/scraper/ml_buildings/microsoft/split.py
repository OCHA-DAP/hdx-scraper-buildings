from re import sub
from shutil import make_archive, rmtree

from duckdb import connect

from .config import GLOBAL_ADMIN1, data_dir


def split_admin1(iso3: str) -> None:
    output_dir = data_dir / f"{iso3}_buildings.gdb".lower()
    partition_dir = output_dir.with_suffix("")
    rmtree(partition_dir, ignore_errors=True)
    with connect() as con:
        con.sql("INSTALL spatial;")
        con.sql("LOAD spatial;")
        con.sql(f"""
            COPY (
                SELECT a.*, b.adm1_name
                FROM ST_Read(
                    '{output_dir}',
                    layer='{iso3.lower()}_buildings'
                ) AS a
                JOIN (
                    SELECT adm1_name, geometry
                    FROM '{GLOBAL_ADMIN1}'
                    WHERE iso_3 = '{iso3}'
                ) as b
                ON ST_Intersects(a.SHAPE, b.geometry)
            ) TO '{partition_dir}' (
                FORMAT 'gdal',
                DRIVER 'OpenFileGDB',
                SRS 'EPSG:4326',
                GEOMETRY_TYPE 'MULTIPOLYGON',
                PARTITION_BY(adm1_name)
            );
        """)
    for partition in partition_dir.iterdir():
        partition_gdb = partition / "data_0.gdb"
        partition_name_raw = (
            partition.name.replace("adm1_name=", "").replace("%20", "_").lower()
        )
        partition_name = (
            "and_" + sub("[^0-9a-zA-Z]+", "_", partition_name_raw) + "_buildings.gdb"
        )
        make_archive(str(partition_dir / partition_name), "zip", partition_gdb)
        rmtree(partition, ignore_errors=True)
