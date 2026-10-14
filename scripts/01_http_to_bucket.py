"""Descarga un Parquet de taxis y carga sus datos en MinIO con dlt."""

import os
from pathlib import Path

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests

PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2025-01.parquet"
)
LOCAL_PARQUET_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "yellow_tripdata_2025-01.parquet"
)
DATASET_NAME = "nyc_taxi_raw"
TABLE_NAME = "yellow_tripdata_2025_01"
BATCH_SIZE = int(os.getenv("PARQUET_BATCH_SIZE", "100000"))
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw")


def choose_minio_endpoint():
    # Jupyter usa el nombre del servicio Docker; la terminal local usa localhost.
    configured = os.getenv("MINIO_ENDPOINT_URL")
    endpoints = [configured] if configured else ["http://minio:9000", "http://localhost:9000"]
    for endpoint in endpoints:
        try:
            with requests.get(f"{endpoint.rstrip('/')}/minio/health/ready", timeout=3) as response:
                if response.status_code == 200:
                    return endpoint
        except requests.RequestException:
            continue
    raise RuntimeError("MinIO no está disponible. Ejecuta: docker compose up -d")


def download_parquet_if_needed():
    # Reutiliza la descarga anterior para no bajar el mismo archivo en cada ejecución.
    LOCAL_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCAL_PARQUET_PATH.exists() and LOCAL_PARQUET_PATH.stat().st_size > 0:
        print(f"Usando archivo local: {LOCAL_PARQUET_PATH}")
        return

    # Descarga por bloques y renombra al terminar para no reutilizar archivos incompletos.
    temporary_path = LOCAL_PARQUET_PATH.with_suffix(".parquet.download")
    print("Descargando dataset...")
    with requests.get(PARQUET_URL, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        with temporary_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)
    temporary_path.replace(LOCAL_PARQUET_PATH)


# replace sustituye la carga anterior para evitar duplicados al repetir el script.
@dlt.resource(table_name=TABLE_NAME, write_disposition="replace")
def yellow_tripdata_batches():
    with pq.ParquetFile(LOCAL_PARQUET_PATH) as parquet_file:
        # Las fechas del dataset se convierten a texto para evitar errores de zonas horarias.
        schema = pa.schema([
            field.with_type(pa.string()) if pa.types.is_timestamp(field.type) else field
            for field in parquet_file.schema_arrow
        ])
        # Lee y entrega un lote a la vez, sin cargar todo el dataset en memoria.
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            yield batch.cast(schema)


def main():
    endpoint = choose_minio_endpoint()
    download_parquet_if_needed()

    # MinIO usa la API S3; los valores por defecto coinciden con docker-compose.yml.
    destination = dlt.destinations.filesystem(
        bucket_url=f"s3://{MINIO_RAW_BUCKET}",
        credentials={
            "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "endpoint_url": endpoint,
            "region_name": os.getenv("MINIO_REGION", "us-east-1"),
        },
    )
    pipeline = dlt.pipeline(
        pipeline_name="http_to_bucket",
        destination=destination,
        dataset_name=DATASET_NAME,
    )

    # dlt escribe los lotes como Parquet en raw/nyc_taxi_raw/yellow_tripdata_2025_01/.
    print(f"Filas del archivo: {pq.read_metadata(LOCAL_PARQUET_PATH).num_rows}")
    print(pipeline.run(yellow_tripdata_batches(), loader_file_format="parquet"))


if __name__ == "__main__":
    main()
