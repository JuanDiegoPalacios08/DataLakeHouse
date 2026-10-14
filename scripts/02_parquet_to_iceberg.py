"""Convierte los Parquet del bucket raw en una tabla Iceberg registrada en Nessie."""

import os
from itertools import chain

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import s3fs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

# El origen coincide con la salida del script 01.
RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME", "nyc_taxi_raw")
RAW_TABLE_NAME = os.getenv("RAW_TABLE_NAME", "yellow_tripdata_2025_01")
WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "corte1")
ICEBERG_TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "yellow_tripdata_2025_01")
ICEBERG_IDENTIFIER = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
ICEBERG_TABLE_LOCATION = f"s3://{WAREHOUSE_BUCKET}/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE_NAME}"
BATCH_SIZE = int(os.getenv("ICEBERG_BATCH_SIZE", "250000"))
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")


def choose_endpoint(service, port, health_path):
    # Permite ejecutar desde Jupyter/Docker o desde la terminal local.
    configured = os.getenv(f"{service.upper()}_ENDPOINT_URL")
    endpoints = [configured] if configured else [f"http://{service}:{port}", f"http://localhost:{port}"]
    for endpoint in endpoints:
        endpoint = endpoint.rstrip("/")
        try:
            with requests.get(f"{endpoint}{health_path}", timeout=5) as response:
                if response.status_code == 200:
                    return endpoint
        except requests.RequestException:
            continue
    raise RuntimeError(f"{service} no está disponible. Ejecuta: docker compose up -d")


def create_iceberg_catalog(nessie_endpoint, minio_endpoint):
    # Nessie registra la tabla; sus datos y metadatos se guardan en MinIO.
    return load_catalog("nessie", **{
        "type": "rest",
        "uri": f"{nessie_endpoint}/iceberg/main",
        "warehouse": WAREHOUSE_BUCKET,
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": minio_endpoint,
        "s3.access-key-id": MINIO_ACCESS_KEY,
        "s3.secret-access-key": MINIO_SECRET_KEY,
        "s3.region": MINIO_REGION,
        "s3.force-virtual-addressing": "false",
    })


def iter_raw_parquet_batches(fs, parquet_files):
    # Lee por lotes para no cargar todos los archivos en memoria.
    for path in parquet_files:
        with fs.open(path, "rb") as file, pq.ParquetFile(file) as parquet_file:
            yield from parquet_file.iter_batches(batch_size=BATCH_SIZE)


def write_batches_to_iceberg(catalog, batches):
    # El primer lote proporciona el esquema de columnas de la tabla.
    try:
        first_batch = next(batches)
    except StopIteration as exc:
        raise RuntimeError("Los Parquet de origen no contienen filas.") from exc

    # Recrea el registro de la tabla para evitar duplicados al repetir la carga.
    try:
        catalog.drop_table(ICEBERG_IDENTIFIER)
    except NoSuchTableError:
        pass
    table = catalog.create_table(
        identifier=ICEBERG_IDENTIFIER,
        schema=first_batch.schema,
        location=ICEBERG_TABLE_LOCATION,
        properties={"format-version": "2"},
    )
    total_rows = 0
    for batch in chain([first_batch], batches):
        table.append(pa.Table.from_batches([batch]))
        total_rows += batch.num_rows
    return total_rows


def main():
    minio_endpoint = choose_endpoint("minio", 9000, "/minio/health/ready")
    nessie_endpoint = choose_endpoint("nessie", 19120, "/api/v2/config")
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": minio_endpoint, "region_name": MINIO_REGION},
    )

    # Busca únicamente los Parquet de la tabla cargada por el script 01.
    parquet_files = sorted(fs.glob(f"{RAW_BUCKET}/{RAW_DATASET_NAME}/{RAW_TABLE_NAME}/*.parquet"))
    if not parquet_files:
        raise RuntimeError("No hay Parquet en raw. Ejecuta primero 01_http_to_bucket.py")
    if not fs.exists(WAREHOUSE_BUCKET):
        fs.mkdir(WAREHOUSE_BUCKET)

    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)
    total_rows = write_batches_to_iceberg(catalog, iter_raw_parquet_batches(fs, parquet_files))
    print(f"Tabla registrada en Nessie: {ICEBERG_IDENTIFIER}")
    print(f"Ubicación en MinIO: {ICEBERG_TABLE_LOCATION}")
    print(f"Filas cargadas: {total_rows}")


if __name__ == "__main__":
    main()
