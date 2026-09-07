from __future__ import annotations

import os
from importlib import resources
from itertools import chain
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs
from pyiceberg.catalog import load_catalog


RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")
WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME", "nyc_taxi_raw")
RAW_TABLE_NAME = os.getenv("RAW_TABLE_NAME", "yellow_tripdata_2025_01")

ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "corte1")
ICEBERG_TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "yellow_tripdata_2025_01")
ICEBERG_IDENTIFIER = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
ICEBERG_TABLE_LOCATION = (
    f"s3://{WAREHOUSE_BUCKET}/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}"
)

BATCH_SIZE = int(os.getenv("ICEBERG_BATCH_SIZE", "250000"))

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")


def configure_timezone_database() -> None:
    """Expose tzdata for libraries that look at TZDIR."""
    if os.getenv("TZDIR"):
        return

    try:
        zoneinfo_dir = resources.files("tzdata").joinpath("zoneinfo")
    except ModuleNotFoundError:
        return

    os.environ["TZDIR"] = str(zoneinfo_dir)


def choose_http_endpoint(
    service_name: str,
    configured_endpoint: str | None,
    docker_endpoint: str,
    local_endpoint: str,
    health_path: str,
) -> str:
    """Choose the service URL that works from Jupyter/Docker or from the host."""
    candidates = []

    if configured_endpoint:
        candidates.append(configured_endpoint)

    candidates.extend([docker_endpoint, local_endpoint])

    for endpoint in dict.fromkeys(candidates):
        endpoint = endpoint.rstrip("/")
        try:
            response = requests.get(f"{endpoint}{health_path}", timeout=5)
            if response.status_code < 500:
                return endpoint
        except requests.RequestException:
            continue

    raise RuntimeError(
        f"No pude conectarme a {service_name}. Revisa que Docker este arriba "
        "con `docker compose ps`."
    )


def choose_minio_endpoint() -> str:
    return choose_http_endpoint(
        service_name="MinIO",
        configured_endpoint=os.getenv("MINIO_ENDPOINT_URL"),
        docker_endpoint="http://minio:9000",
        local_endpoint="http://localhost:9000",
        health_path="/minio/health/ready",
    )


def choose_nessie_endpoint() -> str:
    return choose_http_endpoint(
        service_name="Nessie",
        configured_endpoint=os.getenv("NESSIE_ENDPOINT_URL"),
        docker_endpoint="http://nessie:19120",
        local_endpoint="http://localhost:19120",
        health_path="/api/v2/config",
    )


def create_s3_filesystem(minio_endpoint: str) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={
            "endpoint_url": minio_endpoint,
            "region_name": MINIO_REGION,
        },
    )


def ensure_bucket_exists(
    fs: s3fs.S3FileSystem,
    bucket_name: str,
    *,
    create_if_missing: bool,
) -> None:
    """Validate that a MinIO bucket exists, optionally creating it."""
    if fs.exists(bucket_name):
        print(f"Bucket disponible: s3://{bucket_name}")
        return

    if not create_if_missing:
        raise RuntimeError(
            f"No existe el bucket s3://{bucket_name}. "
            "Ejecuta primero el script anterior o revisa MinIO."
        )

    fs.mkdir(bucket_name)
    print(f"Bucket creado: s3://{bucket_name}")


def find_raw_parquet_files(fs: s3fs.S3FileSystem) -> list[str]:
    """Find the Parquet files written by script 01 in the raw bucket."""
    all_files = fs.find(RAW_BUCKET)
    expected_path_piece = f"{RAW_DATASET_NAME}/{RAW_TABLE_NAME}/"

    parquet_files = sorted(
        path
        for path in all_files
        if path.endswith(".parquet") and expected_path_piece in path
    )

    if not parquet_files:
        raise RuntimeError(
            "No encontre archivos Parquet del primer script en MinIO. "
            "Ejecuta primero `python 01_http_to_bucket.py` y revisa el bucket raw."
        )

    return parquet_files


def iter_raw_parquet_batches(
    fs: s3fs.S3FileSystem,
    parquet_files: list[str],
) -> Iterator[pa.RecordBatch]:
    """Read the raw Parquet files from MinIO using PyArrow batches."""
    for parquet_path in parquet_files:
        print(f"Leyendo archivo raw: s3://{parquet_path}")

        with fs.open(parquet_path, "rb") as file:
            parquet_file = pq.ParquetFile(file)

            for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                yield batch


def create_iceberg_catalog(nessie_endpoint: str, minio_endpoint: str):
    """Connect PyIceberg to Nessie through the Iceberg REST catalog."""
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": f"{nessie_endpoint}/iceberg/main",
            "warehouse": WAREHOUSE_BUCKET,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": minio_endpoint,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": MINIO_REGION,
            "s3.force-virtual-addressing": "false",
        },
    )


def drop_table_if_exists(catalog) -> None:
    """Drop the Iceberg table so rerunning the script does not duplicate rows."""
    try:
        catalog.drop_table(ICEBERG_IDENTIFIER)
        print(f"Tabla Iceberg anterior eliminada: {ICEBERG_IDENTIFIER}")
    except Exception as exc:
        if exc.__class__.__name__ == "NoSuchTableError":
            return
        if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
            return
        raise


def create_namespace_if_needed(catalog) -> None:
    catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)
    print(f"Namespace listo en Nessie: {ICEBERG_NAMESPACE}")


def write_batches_to_iceberg(catalog, batches: Iterator[pa.RecordBatch]) -> int:
    """Create the Iceberg table and append every raw Parquet batch."""
    try:
        first_batch = next(batches)
    except StopIteration as exc:
        raise RuntimeError("Los archivos Parquet encontrados no tienen filas.") from exc

    table = catalog.create_table(
        identifier=ICEBERG_IDENTIFIER,
        schema=first_batch.schema,
        location=ICEBERG_TABLE_LOCATION,
        properties={"format-version": "2"},
    )

    total_rows = 0

    for batch_number, batch in enumerate(chain([first_batch], batches), start=1):
        arrow_table = pa.Table.from_batches([batch], schema=first_batch.schema)
        table.append(arrow_table)
        total_rows += batch.num_rows
        print(
            f"Batch {batch_number} cargado en Iceberg: "
            f"{batch.num_rows} filas. Total acumulado: {total_rows}"
        )

    return total_rows


def print_validation_summary(
    fs: s3fs.S3FileSystem,
    catalog,
    appended_rows: int,
) -> None:
    warehouse_files = fs.find(WAREHOUSE_BUCKET)
    parquet_files = [path for path in warehouse_files if path.endswith(".parquet")]
    metadata_files = [path for path in warehouse_files if "/metadata/" in path]

    print("\nCarga Iceberg terminada.")
    print(f"Tabla registrada: {ICEBERG_IDENTIFIER}")
    print(f"Ubicacion de tabla: {ICEBERG_TABLE_LOCATION}")
    print(f"Filas escritas en Iceberg: {appended_rows}")
    print(f"Archivos Parquet en warehouse: {len(parquet_files)}")
    print(f"Archivos de metadata Iceberg en warehouse: {len(metadata_files)}")

    print("\nNamespaces visibles desde Nessie:")
    print(catalog.list_namespaces())

    print(f"\nTablas visibles en namespace `{ICEBERG_NAMESPACE}`:")
    print(catalog.list_tables(ICEBERG_NAMESPACE))

    print("\nAbre MinIO en http://localhost:9001")
    print(
        "Entra al bucket `warehouse` y busca una ruta parecida a "
        f"`{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}/metadata/`."
    )
    print("Para Nessie, abre http://localhost:19120 o valida con la API.")


def main() -> None:
    configure_timezone_database()

    minio_endpoint = choose_minio_endpoint()
    nessie_endpoint = choose_nessie_endpoint()

    print(f"MinIO endpoint detectado: {minio_endpoint}")
    print(f"Nessie endpoint detectado: {nessie_endpoint}")
    print(f"Buscando Parquet raw en: s3://{RAW_BUCKET}")

    fs = create_s3_filesystem(minio_endpoint)
    ensure_bucket_exists(fs, RAW_BUCKET, create_if_missing=False)
    ensure_bucket_exists(fs, WAREHOUSE_BUCKET, create_if_missing=True)

    raw_parquet_files = find_raw_parquet_files(fs)

    print(f"Archivos Parquet raw encontrados: {len(raw_parquet_files)}")
    print(f"Tabla Iceberg destino: {ICEBERG_IDENTIFIER}")

    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    create_namespace_if_needed(catalog)
    drop_table_if_exists(catalog)

    batches = iter_raw_parquet_batches(fs, raw_parquet_files)
    appended_rows = write_batches_to_iceberg(catalog, batches)

    print_validation_summary(fs, catalog, appended_rows)


if __name__ == "__main__":
    main()
