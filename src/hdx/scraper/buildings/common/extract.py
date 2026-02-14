import logging
from pathlib import Path

from duckdb import connect

from .config import GLOBAL_ADM0

logger = logging.getLogger(__name__)


def extract_country_buildings(iso3: str, input_path: str, output_gpq: Path) -> bool:
    """Fetch buildings for a country from S3 and write to a local parquet.

    Returns False if no source files overlap the country bbox.
    """
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
                WHERE iso3cd = '{iso3}'
                GROUP BY iso3cd
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
                    geometry_bbox.xmax >= getvariable('xmin') AND
                    geometry_bbox.xmin <= getvariable('xmax') AND
                    geometry_bbox.ymax >= getvariable('ymin') AND
                    geometry_bbox.ymin <= getvariable('ymax') AND
                    ST_Intersects(geometry, getvariable('geometry'))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
        count = con.sql(f"SELECT count(*) FROM '{output_gpq}'").fetchone()
    return bool(count and count[0] > 0)
