from __future__ import annotations

import os
import tomllib
from importlib import resources
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

import clickhouse_connect
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

CLICKHOUSE_PIPELINE_NAME = "minio_to_clickhouse"
CLICKHOUSE_DATASET_NAME = os.getenv("CLICKHOUSE_DATASET_NAME", "corte1")
CLICKHOUSE_TABLE_NAME = os.getenv(
    "CLICKHOUSE_TABLE_NAME",
    "yellow_tripdata_2025_01",
)

BATCH_SIZE = int(os.getenv("CLICKHOUSE_BATCH_SIZE", "250000"))
EXPECTED_ROW_COUNT = int(os.getenv("EXPECTED_ROW_COUNT", "3475226"))

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


def normalize_secure(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    return int(str(value).strip().lower() in {"1", "true", "yes", "si"})


def clickhouse_host_is_available(host: str, http_port: int, secure: int) -> bool:
    scheme = "https" if secure else "http"

    try:
        response = requests.get(f"{scheme}://{host}:{http_port}/ping", timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


def read_clickhouse_config() -> tuple[str, dict[str, Any]]:
    """Read ClickHouse destination config from env vars or TOML files."""
    config = load_project_toml("config.toml")
    secrets = load_project_toml("secrets.toml")

    clickhouse_config = (
        nested_get(secrets, "destination", "clickhouse")
        or nested_get(config, "destination", "clickhouse")
        or {}
    )
    credentials_section = clickhouse_config.get("credentials") or {}

    dataset_name = (
        os.getenv("CLICKHOUSE_DATASET_NAME")
        or clickhouse_config.get("dataset_name")
        or CLICKHOUSE_DATASET_NAME
    )
    database = (
        os.getenv("CLICKHOUSE_DATABASE")
        or credentials_section.get("database")
        or "default"
    )
    username = (
        os.getenv("CLICKHOUSE_USER")
        or credentials_section.get("username")
        or "default"
    )
    password = (
        os.getenv("CLICKHOUSE_PASSWORD")
        or credentials_section.get("password")
        or "clickhouse"
    )
    http_port = int(
        os.getenv("CLICKHOUSE_HTTP_PORT")
        or credentials_section.get("http_port")
        or 8123
    )
    secure = normalize_secure(
        os.getenv("CLICKHOUSE_SECURE") or credentials_section.get("secure")
    )

    configured_host = os.getenv("CLICKHOUSE_HOST") or credentials_section.get("host")
    host_candidates = [
        host
        for host in [configured_host, "clickhouse", "localhost"]
        if host
    ]

    selected_host = None
    for host in dict.fromkeys(host_candidates):
        if clickhouse_host_is_available(host, http_port, secure):
            selected_host = host
            break

    if selected_host is None:
        raise RuntimeError(
            "No pude conectarme a ClickHouse. Revisa que `proyecto_clickhouse` "
            "este corriendo con `docker compose ps`."
        )

    native_port = int(
        os.getenv("CLICKHOUSE_NATIVE_PORT")
        or credentials_section.get("port")
        or (9000 if selected_host == "clickhouse" else 9002)
    )

    credentials = {
        "database": database,
        "username": username,
        "password": password,
        "host": selected_host,
        "port": native_port,
        "http_port": http_port,
        "secure": secure,
    }

    return dataset_name, credentials


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


def create_clickhouse_destination(credentials: dict[str, Any]):
    return dlt.destinations.clickhouse(credentials=credentials)


def get_clickhouse_client(credentials: dict[str, Any]):
    return clickhouse_connect.get_client(
        host=credentials["host"],
        port=credentials["http_port"],
        username=credentials["username"],
        password=credentials["password"],
        database=credentials["database"],
        secure=bool(credentials.get("secure", 0)),
    )


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def list_clickhouse_tables(client, database: str) -> list[str]:
    result = client.query(f"SHOW TABLES FROM {quote_identifier(database)}")
    return sorted(row[0] for row in result.result_rows)


def choose_loaded_table(tables: list[str], dataset_name: str) -> str:
    exact_names = [
        CLICKHOUSE_TABLE_NAME,
        f"{dataset_name}___{CLICKHOUSE_TABLE_NAME}",
        f"{dataset_name}_{CLICKHOUSE_TABLE_NAME}",
    ]

    for name in exact_names:
        if name in tables:
            return name

    candidates = [
        table
        for table in tables
        if CLICKHOUSE_TABLE_NAME in table and "_dlt" not in table.lower()
    ]

    if not candidates:
        raise RuntimeError(
            "No pude identificar la tabla cargada en ClickHouse. "
            "Revisa la salida de `SHOW TABLES FROM default`."
        )

    return candidates[0]


def count_clickhouse_table(
    client,
    database: str,
    table_name: str,
) -> int:
    result = client.query(
        "SELECT count() "
        f"FROM {quote_identifier(database)}.{quote_identifier(table_name)}"
    )
    return int(result.result_rows[0][0])


def print_clickhouse_validation(
    credentials: dict[str, Any],
    dataset_name: str,
    expected_rows: int,
) -> None:
    client = get_clickhouse_client(credentials)
    database = credentials["database"]
    tables = list_clickhouse_tables(client, database)
    loaded_table = choose_loaded_table(tables, dataset_name)
    loaded_rows = count_clickhouse_table(client, database, loaded_table)
    metadata_tables = [
        table for table in tables if "dlt" in table.lower()
    ]

    print("\nValidacion en ClickHouse")
    print(f"Base de datos: {database}")
    print(f"SHOW TABLES FROM {database}:")
    for table in tables:
        print(f"  - {table}")

    print(f"\nTabla cargada detectada: {loaded_table}")
    print(f"SELECT count(*) FROM {loaded_table}: {loaded_rows}")

    if loaded_rows == expected_rows:
        print(f"Conteo correcto: {loaded_rows} filas.")
    else:
        print(
            "Advertencia: el conteo no coincide. "
            f"Esperado: {expected_rows}. Obtenido: {loaded_rows}."
        )

    if metadata_tables:
        print("\nTablas de metadata dlt:")
        for table in metadata_tables:
            print(f"  - {table}")


def load_iceberg_data_to_clickhouse(
    fs: s3fs.S3FileSystem,
    data_files: list[str],
    dataset_name: str,
    credentials: dict[str, Any],
) -> None:
    destination = create_clickhouse_destination(credentials)
    pipeline = dlt.pipeline(
        pipeline_name=CLICKHOUSE_PIPELINE_NAME,
        destination=destination,
        dataset_name=dataset_name,
    )

    expected_rows = count_rows_in_iceberg_files(fs, data_files)

    print(f"Destino ClickHouse: {credentials['host']}:{credentials['http_port']}")
    print(f"Base de datos ClickHouse: {credentials['database']}")
    print(f"Dataset dlt en ClickHouse: {dataset_name}")
    print(f"Tabla dlt en ClickHouse: {CLICKHOUSE_TABLE_NAME}")
    print(f"Archivos Iceberg origen: {len(data_files)}")
    print(f"Filas que se cargaran a ClickHouse: {expected_rows}")

    @dlt.resource(table_name=CLICKHOUSE_TABLE_NAME, write_disposition="replace")
    def iceberg_rows_for_clickhouse() -> Iterator[pa.RecordBatch]:
        yield from iter_iceberg_batches(fs, data_files)

    load_info = pipeline.run(
        iceberg_rows_for_clickhouse(),
        loader_file_format="parquet",
    )

    print(load_info)
    print("\nCarga a ClickHouse terminada.")

    print_clickhouse_validation(credentials, dataset_name, expected_rows)


def main() -> None:
    configure_timezone_database()

    minio_endpoint = choose_minio_endpoint()
    nessie_endpoint = choose_nessie_endpoint()
    clickhouse_dataset_name, clickhouse_credentials = read_clickhouse_config()

    print(f"MinIO endpoint detectado: {minio_endpoint}")
    print(f"Nessie endpoint detectado: {nessie_endpoint}")
    print(f"Tabla Iceberg origen: {ICEBERG_IDENTIFIER}")

    fs = create_s3_filesystem(minio_endpoint)
    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    data_files = get_current_iceberg_data_files(catalog)

    load_iceberg_data_to_clickhouse(
        fs=fs,
        data_files=data_files,
        dataset_name=clickhouse_dataset_name,
        credentials=clickhouse_credentials,
    )

    print("\nPara el pantallazo del entregable, conserva esta salida:")
    print("- SHOW TABLES FROM default")
    print(f"- SELECT count(*) = {EXPECTED_ROW_COUNT}")


if __name__ == "__main__":
    main()
