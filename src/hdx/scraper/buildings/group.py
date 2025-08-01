from re import sub
from shutil import make_archive, rmtree
from subprocess import run

from duckdb import connect

from .config import GLOBAL_ADM0, GLOBAL_ADM1, data_dir


def group_by_adm1(provider: str, iso3: str, adm1_id: str, adm_name: str) -> None:
    input_path = data_dir / provider / "outputs" / f"{iso3.lower()}_buildings.parquet"
    output_gpq = (
        input_path.with_suffix("")
        / f"{iso3}_{adm_name}_buildings.parquet".replace("__", "_").lower()
    )
    output_gpq.parent.mkdir(exist_ok=True, parents=True)
    output_gdb = output_gpq.with_suffix(".gdb")
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
    run(
        [
            *["gdal", "vector", "convert"],
            *[output_gpq, output_gdb],
        ],
        check=False,
    )
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
    output_gpq = data_dir / provider / "outputs" / f"{iso3.lower()}_buildings.parquet"
    output_gpq.unlink(missing_ok=True)
    output_gpq.parent.mkdir(exist_ok=True, parents=True)
    output_gdb = output_gpq.with_suffix(".gdb")
    rmtree(output_gdb, ignore_errors=True)
    output_gdb_zip = output_gpq.with_suffix(".gdb.zip")
    with connect() as con:
        con.sql(f"""
            LOAD spatial;
            CREATE TABLE bounds AS (
                SELECT geometry, geometry_bbox AS bbox
                FROM '{GLOBAL_ADM0}'
                WHERE iso_3 = '{iso3}'
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
    run(
        [
            *["gdal", "vector", "convert"],
            *[output_gpq, output_gdb],
        ],
        check=False,
    )
    make_archive(str(output_gdb), "zip", output_gdb)
    rmtree(output_gdb)
    # if output_gdb_zip.stat().st_size > HDX_MAX_SIZE:
    if output_gdb_zip.stat().st_size > 0:
        output_gdb_zip.unlink()
        rmtree(output_gpq.with_suffix(""), ignore_errors=True)
        adm1_info = get_adm1_info(iso3)
        for adm1_id, adm1_src, adm1_name in adm1_info:
            adm_name_safe = ""
            if adm1_src and adm1_name:
                adm_name_safe = (
                    adm1_src.lower()
                    + "_"
                    + sub("[^0-9a-zA-Z]+", "_", adm1_name).lower()
                )
            group_by_adm1(provider, iso3, adm1_id, adm_name_safe)
    output_gpq.unlink()
