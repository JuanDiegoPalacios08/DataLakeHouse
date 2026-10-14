"""Carga la tabla Iceberg en ClickHouse y comprueba que coincida el número de filas."""

import os
import tomllib
from pathlib import Path

import dlt
import requests
from pyiceberg.catalog import load_catalog

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "warehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "corte1")
ICEBERG_TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "yellow_tripdata_2025_01")
ICEBERG_IDENTIFIER = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
CLICKHOUSE_TABLE_NAME = os.getenv("CLICKHOUSE_TABLE_NAME", "yellow_tripdata_2025_01")


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


def load_clickhouse_settings(filename):
    # Conserva la configuración local en .dlt o .dtl, sin incluir secretos en el código.
    for folder in (".dlt", ".dtl"):
        path = PROJECT_ROOT / folder / filename
        if path.exists():
            with path.open("rb") as file:
                return tomllib.load(file).get("destination", {}).get("clickhouse", {})
    return {}


def read_clickhouse_config():
    settings = load_clickhouse_settings("secrets.toml") or load_clickhouse_settings("config.toml")
    saved = settings.get("credentials", {})
    dataset_name = os.getenv("CLICKHOUSE_DATASET_NAME") or settings.get("dataset_name") or "corte1"
    secure = os.getenv("CLICKHOUSE_SECURE") or saved.get("secure", False)
    credentials = {
        "database": os.getenv("CLICKHOUSE_DATABASE") or saved.get("database") or "default",
        "username": os.getenv("CLICKHOUSE_USER") or saved.get("username") or "default",
        "password": os.getenv("CLICKHOUSE_PASSWORD") or saved.get("password") or "clickhouse",
        "http_port": int(os.getenv("CLICKHOUSE_HTTP_PORT") or saved.get("http_port") or 8123),
        "secure": int(str(secure).strip().lower() in ("1", "true", "yes", "si")),
    }
    configured_host = os.getenv("CLICKHOUSE_HOST") or saved.get("host")
    hosts = [configured_host] if configured_host else ["clickhouse", "localhost"]
    scheme = "https" if credentials["secure"] else "http"
    for host in hosts:
        try:
            with requests.get(f"{scheme}://{host}:{credentials['http_port']}/ping", timeout=5) as response:
                if response.status_code == 200:
                    credentials["host"] = host
                    break
        except requests.RequestException:
            continue
    else:
        raise RuntimeError("ClickHouse no está disponible. Ejecuta: docker compose up -d")

    # Docker expone el puerto nativo 9000 del contenedor como 9002 en la máquina local.
    credentials["port"] = int(
        os.getenv("CLICKHOUSE_NATIVE_PORT") or saved.get("port")
        or (9000 if credentials["host"] == "clickhouse" else 9002)
    )
    return dataset_name, credentials


def create_iceberg_catalog(nessie_endpoint, minio_endpoint):
    # Nessie registra la tabla y MinIO almacena sus datos.
    return load_catalog("nessie", **{
        "type": "rest",
        "uri": f"{nessie_endpoint}/iceberg/main",
        "warehouse": WAREHOUSE_BUCKET,
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": minio_endpoint,
        "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "s3.region": os.getenv("MINIO_REGION", "us-east-1"),
        "s3.force-virtual-addressing": "false",
    })


# replace sustituye la carga anterior para evitar duplicados.
@dlt.resource(table_name=CLICKHOUSE_TABLE_NAME, write_disposition="replace")
def iceberg_rows_for_clickhouse(table):
    # Lee el snapshot actual por lotes; las fechas ya son texto desde el script 01.
    with table.scan().to_arrow_batch_reader() as reader:
        yield from reader


def validate_clickhouse(pipeline, expected_rows):
    # Reutiliza el cliente del pipeline para consultar la tabla exacta que cargó dlt.
    with pipeline.sql_client() as client:
        table_name = pipeline.default_schema.naming.normalize_table_identifier(CLICKHOUSE_TABLE_NAME)
        qualified_table = client.make_qualified_table_name(table_name)
        print("Tablas en ClickHouse:")
        for row in client.execute_sql("SHOW TABLES"):
            print(f"  - {row[0]}")
        loaded_rows = client.execute_sql(f"SELECT count(*) FROM {qualified_table}")[0][0]
    print(f"SELECT count(*) FROM {qualified_table}: {loaded_rows}")
    if loaded_rows != expected_rows:
        raise RuntimeError(f"Conteo incorrecto: se esperaban {expected_rows} filas y llegaron {loaded_rows}.")
    print(f"Conteo correcto: {loaded_rows} filas.")


def main():
    dataset_name, credentials = read_clickhouse_config()
    minio_endpoint = choose_endpoint("minio", 9000, "/minio/health/ready")
    nessie_endpoint = choose_endpoint("nessie", 19120, "/api/v2/config")
    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    table = catalog.load_table(ICEBERG_IDENTIFIER)
    expected_rows = table.scan().count()
    if expected_rows == 0:
        raise RuntimeError("La tabla Iceberg no tiene filas. Ejecuta primero 02_parquet_to_iceberg.py")

    # dlt carga los lotes en ClickHouse y después se compara el conteo con Iceberg.
    pipeline = dlt.pipeline(
        pipeline_name="minio_to_clickhouse",
        destination=dlt.destinations.clickhouse(credentials=credentials),
        dataset_name=dataset_name,
    )
    print(f"Filas de origen en Iceberg: {expected_rows}")
    print(pipeline.run(iceberg_rows_for_clickhouse(table), loader_file_format="parquet"))
    validate_clickhouse(pipeline, expected_rows)


if __name__ == "__main__":
    main()
