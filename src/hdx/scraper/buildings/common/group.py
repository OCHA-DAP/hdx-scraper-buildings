from pathlib import Path
from re import sub
from shutil import make_archive, rmtree
from subprocess import run

from duckdb import connect

from .config import AWS_ENDPOINT_S3, GLOBAL_ADM0, GLOBAL_ADM1, HDX_MAX_SIZE


def group_by_adm1(output_dir: Path, iso3: str, adm1_id: str, adm_name: str) -> None:
    """Create a zipped File Geodatabase for a given admin 1 region."""
    input_path = output_dir / f"{iso3.lower()}_buildings.parquet"
    output_name = f"{iso3}_{adm_name}_buildings".replace("__", "_").lower()
    output_gpq = output_dir / f"{output_name}.parquet"
    output_gdb = output_dir / f"{output_name}.gdb"
    with connect() as con:
        con.sql(f"""
            INSTALL spatial;
            LOAD spatial;
            CREATE TABLE bounds AS (
                SELECT
                    geometry,
                    ST_XMin(geometry) AS xmin,
                    ST_YMin(geometry) AS ymin,
                    ST_XMax(geometry) AS xmax,
                    ST_YMax(geometry) AS ymax
                FROM '{GLOBAL_ADM1}'
                WHERE
                    iso_3 = '{iso3}' AND
                    adm1_id = '{adm1_id}'
            );
            SET VARIABLE xmin = (SELECT xmin FROM bounds);
            SET VARIABLE ymin = (SELECT ymin FROM bounds);
            SET VARIABLE xmax = (SELECT xmax FROM bounds);
            SET VARIABLE ymax = (SELECT ymax FROM bounds);
            SET VARIABLE geometry = (SELECT geometry FROM bounds);
            COPY (
                SELECT *
                FROM '{input_path}'
                WHERE
                    ST_XMin(geometry) <= getvariable('xmax') AND
                    ST_XMax(geometry) >= getvariable('xmin') AND
                    ST_YMin(geometry) <= getvariable('ymax') AND
                    ST_YMax(geometry) >= getvariable('ymin') AND
                    ST_Intersects(geometry, getvariable('geometry'))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
    run(["gdal", "vector", "convert", output_gpq, output_gdb, "--quiet"], check=False)
    make_archive(str(output_gdb), "zip", output_gdb)
    rmtree(output_gdb)
    output_gpq.unlink()


def get_adm1_info(iso3: str) -> list[tuple[str, str, str]]:
    """Get information about admin 1 regions for a given country."""
    with connect() as con:
        return con.sql(f"""
            SELECT adm1_id, adm1_src, adm1_name
            FROM '{GLOBAL_ADM1}'
            WHERE iso_3 = '{iso3}'
        """).fetchall()


def group(provider: str, iso3: str, output_dir: Path) -> None:
    """Create a zipped File Geodatabase for a given country."""
    input_path = f"s3://{AWS_ENDPOINT_S3}/hdx/{provider}-open-buildings/**/*.parquet"
    output_name = f"{iso3.lower()}_buildings"
    rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_gpq = output_dir / f"{output_name}.parquet"
    output_gdb = output_dir / f"{output_name}.gdb"
    output_gdb_zip = output_dir / f"{output_name}.gdb.zip"
    with connect() as con:
        con.sql(f"""
            INSTALL spatial;
            LOAD spatial;
            INSTALL httpfs;
            LOAD httpfs;
            CREATE SECRET (TYPE s3, KEY_ID '', SECRET '');

            CREATE TABLE bounds AS (
                SELECT
                    ST_MemUnion_Agg(geometry) AS geometry,
                    min(ST_XMin(geometry)) AS xmin,
                    min(ST_YMin(geometry)) AS ymin,
                    max(ST_XMax(geometry)) AS xmax,
                    max(ST_YMax(geometry)) AS ymax
                FROM '{GLOBAL_ADM0}'
                WHERE iso_3 = '{iso3}'
                GROUP BY iso_3
            );

            SET VARIABLE xmin = (SELECT xmin FROM bounds);
            SET VARIABLE ymin = (SELECT ymin FROM bounds);
            SET VARIABLE xmax = (SELECT xmax FROM bounds);
            SET VARIABLE ymax = (SELECT ymax FROM bounds);
            SET VARIABLE geometry = (SELECT geometry FROM bounds);

            COPY (
                SELECT *
                FROM '{input_path}'
                WHERE
                    ST_XMin(geometry) <= getvariable('xmax') AND
                    ST_XMax(geometry) >= getvariable('xmin') AND
                    ST_YMin(geometry) <= getvariable('ymax') AND
                    ST_YMax(geometry) >= getvariable('ymin') AND
                    ST_Intersects(geometry, getvariable('geometry'))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
    run(["gdal", "vector", "convert", output_gpq, output_gdb, "--quiet"], check=False)
    make_archive(str(output_gdb), "zip", output_gdb)
    rmtree(output_gdb)
    if output_gdb_zip.stat().st_size > HDX_MAX_SIZE:
        output_gdb_zip.unlink()
        adm1_info = get_adm1_info(iso3)
        for adm1_id, adm1_src, adm1_name in adm1_info:
            adm_name = ""
            if adm1_src and adm1_name:
                adm_name_combined = f"{adm1_src}_{adm1_name}"
                adm_name = sub("[^0-9a-zA-Z]+", "_", adm_name_combined).lower()
            group_by_adm1(output_dir, iso3, adm1_id, adm_name)
    output_gpq.unlink(missing_ok=True)
