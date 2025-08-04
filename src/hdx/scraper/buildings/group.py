from pathlib import Path
from re import sub
from shutil import make_archive, rmtree
from subprocess import run

from duckdb import connect

from .config import GLOBAL_ADM0, GLOBAL_ADM1, HDX_MAX_SIZE, data_dir


def group_by_adm1(output_dir: Path, iso3: str, adm1_id: str, adm_name: str) -> None:
    input_path = output_dir / f"{iso3.lower()}_buildings.parquet"
    output_name = f"{iso3}_{adm_name}_buildings".replace("__", "_").lower()
    output_gpq = output_dir / f"{output_name}.parquet"
    output_gdb = output_dir / f"{output_name}.gdb"
    with connect() as con:
        con.sql(f"""
            LOAD spatial;
            CREATE TABLE bounds AS (
                SELECT geometry, geometry_bbox AS bbox
                FROM '{GLOBAL_ADM1}'
                WHERE
                    iso_3 = '{iso3}' AND
                    adm1_id = '{adm1_id}'
            );
            SET VARIABLE xmin = (SELECT bbox.xmin FROM bounds);
            SET VARIABLE ymin = (SELECT bbox.ymin FROM bounds);
            SET VARIABLE xmax = (SELECT bbox.xmax FROM bounds);
            SET VARIABLE ymax = (SELECT bbox.ymax FROM bounds);
            SET VARIABLE geometry = (SELECT geometry FROM bounds);
            COPY (
                SELECT *
                FROM '{input_path}'
                WHERE
                    bbox.xmin > getvariable('xmin') AND
                    bbox.xmax < getvariable('xmax') AND
                    bbox.ymin > getvariable('ymin') AND
                    bbox.ymax < getvariable('ymax') AND
                    ST_Intersects(geometry, getvariable('geometry'))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
    run(["gdal", "vector", "convert", output_gpq, output_gdb], check=False)
    make_archive(str(output_gdb), "zip", output_gdb)
    rmtree(output_gdb)
    output_gpq.unlink()


def get_adm1_info(iso3: str) -> list[tuple[str, str, str]]:
    with connect() as con:
        return con.sql(f"""
            SELECT adm1_id, adm1_src, adm1_name
            FROM '{GLOBAL_ADM1}'
            WHERE iso_3 = '{iso3}'
        """).fetchall()


def group_by_adm0(provider: str, iso3: str) -> None:
    input_path = data_dir / provider / "inputs" / "**/*.parquet"
    output_dir = data_dir / provider / "outputs" / iso3.lower()
    output_name = f"{iso3.lower()}_buildings"
    rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)
    output_gpq = output_dir / f"{output_name}.parquet"
    output_gdb = output_dir / f"{output_name}.gdb"
    output_gdb_zip = output_dir / f"{output_name}.gdb.zip"
    with connect() as con:
        con.sql(f"""
            LOAD spatial;
            CREATE TABLE bounds AS (
                SELECT geometry, geometry_bbox AS bbox
                FROM '{GLOBAL_ADM0}'
                WHERE iso_3 = '{iso3}'
                LIMIT 1
            );
            SET VARIABLE xmin = (SELECT bbox.xmin FROM bounds);
            SET VARIABLE ymin = (SELECT bbox.ymin FROM bounds);
            SET VARIABLE xmax = (SELECT bbox.xmax FROM bounds);
            SET VARIABLE ymax = (SELECT bbox.ymax FROM bounds);
            SET VARIABLE geometry = (SELECT geometry FROM bounds);
            COPY (
                SELECT * RENAME (geometry_bbox AS bbox)
                FROM '{input_path}'
                WHERE
                    geometry_bbox.xmin > getvariable('xmin') AND
                    geometry_bbox.xmax < getvariable('xmax') AND
                    geometry_bbox.ymin > getvariable('ymin') AND
                    geometry_bbox.ymax < getvariable('ymax') AND
                    ST_Intersects(geometry, getvariable('geometry'))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
    run(["gdal", "vector", "convert", output_gpq, output_gdb], check=False)
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
