from math import ceil
from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import run

from duckdb import connect

from .config import HDX_MAX_SIZE


def split_into_parts(output_dir: Path, iso3: str, zip_size: int) -> None:
    """Split the country GDB into the fewest parts that each fit within HDX_MAX_SIZE."""
    input_path = output_dir / f"{iso3.lower()}_buildings.parquet"
    num_parts = ceil(zip_size / HDX_MAX_SIZE)

    with connect() as con:
        row = con.sql(f"SELECT COUNT(*) FROM '{input_path}'").fetchone()
        total_rows = row[0] if row else 0

    rows_per_part = ceil(total_rows / num_parts)

    for part_num in range(1, num_parts + 1):
        offset = (part_num - 1) * rows_per_part
        output_name = f"{iso3.lower()}_buildings_part{part_num}"
        output_gpq = output_dir / f"{output_name}.parquet"
        output_gdb = output_dir / f"{output_name}.gdb"

        with connect() as con:
            con.sql("INSTALL spatial; LOAD spatial;")
            con.sql(f"""
                COPY (
                    SELECT * FROM '{input_path}'
                    LIMIT {rows_per_part} OFFSET {offset}
                )
                TO '{output_gpq}'
                WITH (COMPRESSION zstd);
            """)
        run(
            ["gdal", "vector", "convert", output_gpq, output_gdb, "--quiet"],
            check=False,
        )
        make_archive(str(output_gdb), "zip", output_gdb)
        rmtree(output_gdb)
        output_gpq.unlink()
