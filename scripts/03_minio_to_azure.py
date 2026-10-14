
"""Lee la tabla Iceberg de MinIO y exporta sus datos como Parquet a Azure."""

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
AZURE_GROUP = os.getenv("AZURE_GROUP", "GRUPO_5")
AZURE_DATASET_NAME = "iceberg_to_azure"
AZURE_TABLE_NAME = "yellow_tripdata_2025_01"


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


def load_azure_settings(filename):
    # Acepta la carpeta estándar .dlt y la carpeta .dtl que ya utiliza el proyecto.
    for folder in (".dlt", ".dtl"):
        path = PROJECT_ROOT / folder / filename
        if path.exists():
            with path.open("rb") as file:
                data = tomllib.load(file)
            settings = data.get("destination", {}).get("filesystem", {})
            legacy = data.get("s3_to_adls", {}).get("destination", {}).get("filesystem", {})
            return {
                "bucket_url": legacy.get("bucket_url") or settings.get("bucket_url"),
                "credentials": legacy.get("credentials") or settings.get("credentials", {}),
            }
    return {}


def read_azure_config():
    config = load_azure_settings("config.toml")
    secrets = load_azure_settings("secrets.toml")
    bucket_url = (
        os.getenv("AZURE_BUCKET_URL") or os.getenv("DESTINATION__FILESYSTEM__BUCKET_URL")
        or secrets.get("bucket_url") or config.get("bucket_url")
        or "abfss://clase-4-dlt@fhbd.dfs.core.windows.net"
    ).strip().rstrip("/")

    # Las variables de entorno tienen prioridad sobre las credenciales del archivo.
    credentials = dict(secrets.get("credentials", {}))
    for variable in ("AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT_KEY", "AZURE_STORAGE_SAS_TOKEN"):
        value = os.getenv(variable) or os.getenv(f"DESTINATION__FILESYSTEM__CREDENTIALS__{variable}")
        if value:
            credentials[variable.lower()] = value
    if not credentials.get("azure_storage_account_name"):
        raise RuntimeError("Falta la cuenta de Azure en secrets.toml o AZURE_STORAGE_ACCOUNT_NAME.")
    if not (credentials.get("azure_storage_account_key") or credentials.get("azure_storage_sas_token")):
        raise RuntimeError("Falta la clave o el token SAS de Azure en secrets.toml o variables de entorno.")

    # Sustituye una carpeta GRUPO anterior o añade la del grupo configurado.
    if bucket_url.rsplit("/", 1)[-1].startswith("GRUPO_"):
        bucket_url = bucket_url.rsplit("/", 1)[0]
    return f"{bucket_url}/{AZURE_GROUP}", credentials


def create_iceberg_catalog(nessie_endpoint, minio_endpoint):
    # Nessie indica dónde está la tabla; MinIO almacena sus archivos.
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


# replace sustituye la exportación anterior para evitar duplicados.
@dlt.resource(table_name=AZURE_TABLE_NAME, write_disposition="replace")
def iceberg_rows_for_azure(table):
    # Iceberg lee el snapshot actual por lotes; las fechas ya son texto desde el script 01.
    with table.scan().to_arrow_batch_reader() as reader:
        yield from reader


def main():
    azure_bucket_url, credentials = read_azure_config()
    minio_endpoint = choose_endpoint("minio", 9000, "/minio/health/ready")
    nessie_endpoint = choose_endpoint("nessie", 19120, "/api/v2/config")
    catalog = create_iceberg_catalog(nessie_endpoint, minio_endpoint)
    table = catalog.load_table(ICEBERG_IDENTIFIER)
    if not list(table.scan().plan_files()):
        raise RuntimeError("La tabla Iceberg no tiene datos. Ejecuta primero 02_parquet_to_iceberg.py")

    # dlt escribe los lotes como Parquet dentro de la carpeta del grupo en Azure.
    destination = dlt.destinations.filesystem(bucket_url=azure_bucket_url, credentials=credentials)
    pipeline = dlt.pipeline(
        pipeline_name="minio_to_azure",
        destination=destination,
        dataset_name=AZURE_DATASET_NAME,
    )
    print(f"Tabla de origen: {ICEBERG_IDENTIFIER}")
    print(f"Destino Azure: {azure_bucket_url}/{AZURE_DATASET_NAME}/{AZURE_TABLE_NAME}/")
    print(pipeline.run(iceberg_rows_for_azure(table), loader_file_format="parquet"))


if __name__ == "__main__":
    main()
