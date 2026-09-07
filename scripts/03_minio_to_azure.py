from __future__ import annotations

import os
import tomllib
from importlib import resources
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
import s3fs
from pyiceberg.catalog import load_catalog


PROJECT_ROOT = Path(__file__).resolve().parents[1]

WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "corte1")
ICEBERG_TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "yellow_tripdata_2025_01")
ICEBERG_IDENTIFIER = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"

AZURE_GROUP = os.getenv("AZURE_GROUP", "GRUPO_5")
AZURE_DEFAULT_BUCKET_URL = (
    f"abfss://clase-4-dlt@fhbd.dfs.core.windows.net/{AZURE_GROUP}"
)
AZURE_PIPELINE_NAME = "minio_to_azure"
AZURE_DATASET_NAME = "iceberg_to_azure"
AZURE_TABLE_NAME = "yellow_tripdata_2025_01"

BATCH_SIZE = int(os.getenv("AZURE_BATCH_SIZE", "250000"))

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


def load_toml_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("rb") as file:
        return tomllib.load(file)


def load_project_toml(file_name: str) -> dict[str, Any]:
    """Read a project TOML file from .dlt or .dtl."""
    for folder_name in [".dlt", ".dtl"]:
        path = PROJECT_ROOT / folder_name / file_name
        if path.exists():
            print(f"Usando {file_name} desde: {path}")
            return load_toml_if_exists(path)

    return {}


def nested_get(data: dict[str, Any], *keys: str) -> Any:
    current: Any = data

    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)

    return current


def normalize_azure_bucket_url(bucket_url: str | None) -> str:
    """Force the destination folder to the group assigned for this project."""
    if not bucket_url:
        return AZURE_DEFAULT_BUCKET_URL

    bucket_url = bucket_url.strip().rstrip("/")

    if "/GRUPO_" in bucket_url:
        return bucket_url.rsplit("/", 1)[0] + f"/{AZURE_GROUP}"

    return f"{bucket_url}/{AZURE_GROUP}"


def read_azure_config() -> tuple[str, dict[str, str]]:
    """Read Azure destination URL and credentials from env vars or secrets.toml."""
    config = load_project_toml("config.toml")
    secrets = load_project_toml("secrets.toml")

    bucket_url = (
        os.getenv("AZURE_BUCKET_URL")
        or os.getenv("DESTINATION__FILESYSTEM__BUCKET_URL")
        or nested_get(secrets, "s3_to_adls", "destination", "filesystem", "bucket_url")
        or nested_get(secrets, "destination", "filesystem", "bucket_url")
        or nested_get(config, "s3_to_adls", "destination", "filesystem", "bucket_url")
        or nested_get(config, "destination", "filesystem", "bucket_url")
        or AZURE_DEFAULT_BUCKET_URL
    )

    credentials_section = (
        nested_get(secrets, "s3_to_adls", "destination", "filesystem", "credentials")
        or nested_get(secrets, "destination", "filesystem", "credentials")
        or {}
    )

    account_name = (
        os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        or os.getenv(
            "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"
        )
        or credentials_section.get("azure_storage_account_name")
    )
    account_key = (
        os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        or os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY")
        or credentials_section.get("azure_storage_account_key")
    )
    sas_token = (
        os.getenv("AZURE_STORAGE_SAS_TOKEN")
        or os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_SAS_TOKEN")
        or credentials_section.get("azure_storage_sas_token")
    )

    credentials: dict[str, str] = {}

    if account_name:
        credentials["azure_storage_account_name"] = account_name
    if account_key:
        credentials["azure_storage_account_key"] = account_key
    if sas_token:
        credentials["azure_storage_sas_token"] = sas_token

    if "azure_account_host" in credentials_section:
        credentials["azure_account_host"] = credentials_section["azure_account_host"]

    if "azure_storage_account_name" not in credentials:
        raise RuntimeError(
            "Falta `azure_storage_account_name`. Configuralo en `.dlt/secrets.toml`, "
            "`.dtl/secrets.toml` o en la variable AZURE_STORAGE_ACCOUNT_NAME."
        )

    if (
        "azure_storage_account_key" not in credentials
        and "azure_storage_sas_token" not in credentials
    ):
        raise RuntimeError(
            "Falta la clave o SAS token de Azure. Configura "
            "`azure_storage_account_key` en el archivo de secretos, "
            "o usa la variable AZURE_STORAGE_ACCOUNT_KEY."
        )

    return normalize_azure_bucket_url(bucket_url), credentials


def create_s3_filesystem(minio_endpoint: str) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={
            "endpoint_url": minio_endpoint,
            "region_name": MINIO_REGION,
        },
    )


def create_iceberg_catalog(nessie_endpoint: str, minio_endpoint: str):
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


def strip_s3_scheme(file_path: str) -> str:
    parsed = urlparse(file_path)

    if parsed.scheme != "s3":
        return file_path

    return f"{parsed.netloc}{parsed.path}"


def get_current_iceberg_data_files(catalog) -> list[str]:
    """Ask Iceberg which Parquet files belong to the current table snapshot."""
    table = catalog.load_table(ICEBERG_IDENTIFIER)
    files = sorted(
        strip_s3_scheme(task.file.file_path)
        for task in table.scan().plan_files()
        if task.file.file_path.endswith(".parquet")
    )

    if not files:
        raise RuntimeError(
            f"La tabla Iceberg {ICEBERG_IDENTIFIER} no tiene archivos Parquet. "
            "Ejecuta primero `python 02_parquet_to_iceberg.py`."
        )

    return files


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


def iter_iceberg_batches(
    fs: s3fs.S3FileSystem,
    data_files: list[str],
) -> Iterator[pa.RecordBatch]:
    """Read the current Iceberg Parquet data files from MinIO."""
    for data_file in data_files:
        print(f"Leyendo archivo Iceberg: s3://{data_file}")

        with fs.open(data_file, "rb") as file:
            parquet_file = pq.ParquetFile(file)

            for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                yield prepare_batch_for_dlt(batch)


def count_rows_in_iceberg_files(
    fs: s3fs.S3FileSystem,
    data_files: list[str],
) -> int:
    total_rows = 0

    for data_file in data_files:
        with fs.open(data_file, "rb") as file:
            total_rows += pq.ParquetFile(file).metadata.num_rows

    return total_rows


def load_iceberg_data_to_azure(
    fs: s3fs.S3FileSystem,
    data_files: list[str],
    azure_bucket_url: str,
    azure_credentials: dict[str, str],
) -> None:
    destination = dlt.destinations.filesystem(
        bucket_url=azure_bucket_url,
        credentials=azure_credentials,
    )
    pipeline = dlt.pipeline(
        pipeline_name=AZURE_PIPELINE_NAME,
        destination=destination,
        dataset_name=AZURE_DATASET_NAME,
    )

    expected_rows = count_rows_in_iceberg_files(fs, data_files)

    print(f"Destino Azure: {azure_bucket_url}")
    print(f"Dataset dlt en Azure: {AZURE_DATASET_NAME}")
    print(f"Tabla dlt en Azure: {AZURE_TABLE_NAME}")
    print(f"Archivos Iceberg origen: {len(data_files)}")
    print(f"Filas que se enviaran a Azure: {expected_rows}")

    @dlt.resource(table_name=AZURE_TABLE_NAME, write_disposition="replace")
    def iceberg_rows_for_azure() -> Iterator[pa.RecordBatch]:
        yield from iter_iceberg_batches(fs, data_files)

    load_info = pipeline.run(
        iceberg_rows_for_azure(),
        loader_file_format="parquet",
    )

    print(load_info)
    print("\nCarga a Azure terminada.")
    print(f"Carpeta del grupo: {AZURE_GROUP}")
    print(
        "Busca en Azure Storage una ruta parecida a "
        f"`{AZURE_GROUP}/{AZURE_DATASET_NAME}/{AZURE_TABLE_NAME}/`."
    )


def main() -> None:
    configure_timezone_database()

    minio_endpoint = choose_minio_endpoint()
    nessie_endpoint = choose_nessie_endpoint()
    azure_bucket_url, azure_credentials = read_azure_config()

    print(f"MinIO endpoint detectado: {minio_endpoint}")
    print(f"Nessie endpoint detectado: {nessie_endpoint}")
    print(f"Tabla Iceberg origen: {ICEBERG_IDENTIFIER}")

    fs = create_s3_filesystem(minio_endpoint)
    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    data_files = get_current_iceberg_data_files(catalog)

    load_iceberg_data_to_azure(
        fs=fs,
        data_files=data_files,
        azure_bucket_url=azure_bucket_url,
        azure_credentials=azure_credentials,
    )


if __name__ == "__main__":
    main()
