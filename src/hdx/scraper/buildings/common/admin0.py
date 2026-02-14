from pathlib import Path
from subprocess import run
from urllib.parse import urlencode

from httpx import Client

from .config import (
    ARCGIS_ADM0_URL,
    ARCGIS_PASSWORD,
    ARCGIS_SERVER,
    ARCGIS_USERNAME,
    TIMEOUT,
)

_ARCGIS_OBJECTID = "esriFieldTypeOID"


def download_admin0(data_dir: Path) -> None:
    """Download Admin 0 from ArcGIS Feature Services."""
    token_url = f"{ARCGIS_SERVER}/portal/sharing/rest/generateToken"
    with Client(http2=True, timeout=TIMEOUT) as client:
        token = client.post(
            token_url,
            data={
                "username": ARCGIS_USERNAME,
                "password": ARCGIS_PASSWORD,
                "referer": f"{ARCGIS_SERVER}/portal",
                "expiration": 1440,
                "f": "json",
            },
        ).json()["token"]
        params = {"f": "json", "token": token}
        fields = client.get(ARCGIS_ADM0_URL, params=params).json()["fields"]
    objectid = next(x["name"] for x in fields if x["type"] == _ARCGIS_OBJECTID)
    field_names = ",".join(
        x["name"]
        for x in fields
        if x["type"] != _ARCGIS_OBJECTID
        and not x.get("virtual")
        and not x["name"].lower().startswith("objectid")
    )
    query_params = urlencode(
        {**params, "orderByFields": objectid, "outFields": field_names, "where": "1=1"},
    )
    query_url = f"{ARCGIS_ADM0_URL}/query?{query_params}"
    output_file = data_dir / "bnda_cty.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            *["gdal", "vector", "pipeline", "!"],
            *["read", "ESRIJSON:" + query_url, "!"],
            *["reproject", "--dst-crs=EPSG:4326", "!"],
            *["clean-coverage", "!"],
            *["make-valid", "!"],
            *["write", str(output_file)],
            "--overwrite",
            "--quiet",
            "--lco=COMPRESSION_LEVEL=15",
            "--lco=COMPRESSION=ZSTD",
            "--lco=GEOMETRY_NAME=geometry",
            "--lco=USE_PARQUET_GEO_TYPES=YES",
        ],
        check=False,
    )
