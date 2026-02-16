import logging
from pathlib import Path

from duckdb import connect

from .config import GLOBAL_ADM0

logger = logging.getLogger(__name__)


def extract_country_buildings(iso3: str, input_path: str, output_gpq: Path) -> bool:
    """Fetch buildings for a country from S3 and write to a local parquet.

    Returns False if no source files overlap the country bbox.
    Uses a Python round-trip to fetch bbox values before the COPY query.
    """
    with connect() as con:
        con.sql(f"""
            INSTALL spatial; LOAD spatial;
            INSTALL httpfs; LOAD httpfs;
            CREATE SECRET (TYPE s3, KEY_ID '', SECRET '');
            CREATE TEMP TABLE adm0 AS
            SELECT
                ST_MemUnion_Agg(geometry) AS geometry,
                min(ST_XMin(geometry)) AS xmin,
                min(ST_YMin(geometry)) AS ymin,
                max(ST_XMax(geometry)) AS xmax,
                max(ST_YMax(geometry)) AS ymax
            FROM '{GLOBAL_ADM0}'
            WHERE iso3cd = '{iso3}'
            GROUP BY iso3cd;
        """)
        xmin, ymin, xmax, ymax = con.sql(
            "SELECT xmin, ymin, xmax, ymax FROM adm0"
        ).fetchone()  # type: ignore  # noqa: PGH003
        con.sql(f"""
            COPY (
                SELECT * RENAME (geometry_bbox AS bbox)
                FROM read_parquet('{input_path}')
                WHERE
                    geometry_bbox.xmax >= {xmin} AND
                    geometry_bbox.xmin <= {xmax} AND
                    geometry_bbox.ymax >= {ymin} AND
                    geometry_bbox.ymin <= {ymax} AND
                    ST_Intersects(geometry, (SELECT geometry FROM adm0))
            )
            TO '{output_gpq}'
            WITH (COMPRESSION zstd);
        """)
        count = con.sql(f"SELECT count(*) FROM '{output_gpq}'").fetchone()
    return bool(count and count[0] > 0)
