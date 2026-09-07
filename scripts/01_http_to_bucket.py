from __future__ import annotations

import os
from importlib import resources
from pathlib import Path
from typing import Iterable

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests


PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2025-01.parquet"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
LOCAL_PARQUET_PATH = DATA_DIR / "yellow_tripdata_2025-01.parquet"

PIPELINE_NAME = "http_to_bucket"
DATASET_NAME = "nyc_taxi_raw"
TABLE_NAME = "yellow_tripdata_2025_01"
BATCH_SIZE = int(os.getenv("PARQUET_BATCH_SIZE", "100000"))

MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")


def configure_pyarrow_timezone_database() -> None:
    """Expose tzdata to libraries that can read TZDIR."""
    if os.getenv("TZDIR"):
        return

    try:
        zoneinfo_dir = resources.files("tzdata").joinpath("zoneinfo")
    except ModuleNotFoundError:
        print(
            "Advertencia: no esta instalado `tzdata`. "
            "Si ves errores con timezone UTC, ejecuta `pip install tzdata`."
        )
        return

    os.environ["TZDIR"] = str(zoneinfo_dir)


def choose_minio_endpoint() -> str:
    """Return a reachable MinIO endpoint for Docker/Jupyter or local VS Code."""
    configured_endpoint = os.getenv("MINIO_ENDPOINT_URL")
    candidates = []

    if configured_endpoint:
        candidates.append(configured_endpoint)

    candidates.extend(
        [
            "http://minio:9000",  # Works from Jupyter because both services are in Docker.
            "http://localhost:9000",  # Works from VS Code / terminal on your host machine.
        ]
    )

    for endpoint in dict.fromkeys(candidates):
        endpoint = endpoint.rstrip("/")
        try:
            response = requests.get(
                f"{endpoint}/minio/health/ready",
                timeout=3,
            )
            if response.status_code < 500:
                return endpoint
        except requests.RequestException:
            continue

    raise RuntimeError(
        "No pude conectarme a MinIO. Primero ejecuta `docker compose up -d` "
        "y revisa que el servicio `proyecto_minio` este corriendo."
    )


def download_parquet_if_needed() -> None:
    """Download the public Parquet file once and keep a local cache."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if LOCAL_PARQUET_PATH.exists() and LOCAL_PARQUET_PATH.stat().st_size > 0:
        print(f"Usando archivo local existente: {LOCAL_PARQUET_PATH}")
        return

    temp_path = LOCAL_PARQUET_PATH.with_suffix(".parquet.download")
    print(f"Descargando dataset desde HTTP:\n{PARQUET_URL}")

    with requests.get(PARQUET_URL, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()

        with temp_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    temp_path.replace(LOCAL_PARQUET_PATH)
    print(f"Archivo descargado en: {LOCAL_PARQUET_PATH}")


def prepare_batch_for_dlt(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert temporal columns to text to avoid timezone issues in minimal containers."""
    arrays = []
    fields = []

    for field, column in zip(batch.schema, batch.columns):
        if (
            pa.types.is_timestamp(field.type)
            or pa.types.is_date(field.type)
            or pa.types.is_time(field.type)
        ):
            arrays.append(pc.cast(column, pa.string()))
            fields.append(pa.field(field.name, pa.string()))
        else:
            arrays.append(column)
            fields.append(field)

    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


@dlt.resource(table_name=TABLE_NAME, write_disposition="replace")
def yellow_tripdata_batches() -> Iterable[pa.RecordBatch]:
    """Yield the Parquet file in Arrow batches so dlt can write Parquet to MinIO."""
    parquet_file = pq.ParquetFile(LOCAL_PARQUET_PATH)

    for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
        yield prepare_batch_for_dlt(batch)


def load_parquet_to_minio() -> None:
    """Run the dlt pipeline that writes the HTTP dataset into the MinIO raw bucket."""
    minio_endpoint = choose_minio_endpoint()
    parquet_file = pq.ParquetFile(LOCAL_PARQUET_PATH)
    total_rows = parquet_file.metadata.num_rows

    destination = dlt.destinations.filesystem(
        bucket_url=f"s3://{MINIO_RAW_BUCKET}",
        credentials={
            "aws_access_key_id": MINIO_ACCESS_KEY,
            "aws_secret_access_key": MINIO_SECRET_KEY,
            "endpoint_url": minio_endpoint,
            "region_name": MINIO_REGION,
        },
    )

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=destination,
        dataset_name=DATASET_NAME,
    )

    print(f"MinIO endpoint detectado: {minio_endpoint}")
    print(f"Bucket destino: s3://{MINIO_RAW_BUCKET}")
    print(f"Dataset dlt: {DATASET_NAME}")
    print(f"Tabla dlt: {TABLE_NAME}")
    print(f"Filas detectadas en el Parquet original: {total_rows}")

    load_info = pipeline.run(
        yellow_tripdata_batches(),
        loader_file_format="parquet",
    )

    print(load_info)
    print("\nCarga terminada.")
    print("Abre MinIO en http://localhost:9001")
    print(
        "Busca el bucket `raw` y entra a una ruta parecida a "
        f"`{DATASET_NAME}/{TABLE_NAME}/` para ver los archivos Parquet."
    )


def main() -> None:
    configure_pyarrow_timezone_database()
    download_parquet_if_needed()
    load_parquet_to_minio()


if __name__ == "__main__":
    main()
