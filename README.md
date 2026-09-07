# Data Lakehouse

Este proyecto implementa un flujo de datos tipo **lakehouse** usando herramientas modernas para almacenamiento, catalogacion, transferencia y consulta analitica de datos.

El objetivo es tomar un dataset en formato Parquet desde una fuente HTTP, almacenarlo en un data lake local, organizarlo como una tabla Apache Iceberg, registrar esa tabla en un catalogo Nessie, enviarlo a Azure Storage y finalmente cargarlo en ClickHouse para su consulta.

## Arquitectura

El flujo general del proyecto es:

```text
Fuente HTTP
    |
    v
MinIO - Data Lake
    |
    v
Apache Iceberg - Table Format
    |
    v
Nessie - Catalogo
    |
    +--> Azure Storage
    |
    +--> ClickHouse - Data Warehouse
```

## Tecnologias utilizadas

- **Docker Compose**: orquesta los servicios necesarios para ejecutar el proyecto localmente.
- **Jupyter**: ambiente usado para ejecutar los scripts Python.
- **MinIO**: almacenamiento compatible con S3 usado como data lake.
- **Apache Parquet**: formato columnar usado para almacenar los datos.
- **Apache Iceberg**: formato de tabla usado para organizar los archivos del lakehouse.
- **Nessie**: catalogo que permite registrar y consultar la tabla Iceberg.
- **Azure Storage**: destino externo donde se envia una copia de los datos procesados.
- **ClickHouse**: base de datos analitica usada para validar y consultar los datos finales.
- **dlt**: herramienta usada para mover y cargar datos entre los diferentes destinos.
- **PyArrow**: libreria usada para leer y procesar archivos Parquet.

## Flujo del proyecto

El proyecto esta dividido en cuatro scripts principales:

### 1. Carga desde HTTP hacia MinIO

Script:

```text
scripts/01_http_to_bucket.py
```

Este script descarga el dataset desde una fuente HTTP y lo guarda en MinIO dentro de una zona inicial de datos crudos. Esta etapa representa la ingesta inicial del pipeline.

### 2. Conversion de Parquet a tabla Iceberg

Script:

```text
scripts/02_parquet_to_iceberg.py
```

Este script lee los datos almacenados en MinIO y crea una tabla Apache Iceberg. La tabla queda almacenada en el data lake y registrada en Nessie.

En esta etapa se pasa de tener archivos Parquet sueltos a tener una tabla lakehouse administrada con metadata.

### 3. Envio de datos hacia Azure Storage

Script:

```text
scripts/03_minio_to_azure.py
```

Este script toma los datos actuales de la tabla Iceberg y los envia a Azure Storage. La configuracion de Azure se maneja de forma local mediante archivos de configuracion privados o variables de entorno.

Las credenciales no deben subirse al repositorio.

### 4. Carga de datos hacia ClickHouse

Script:

```text
scripts/04_minio_to_clickhouse.py
```

Este script lee los datos de la tabla Iceberg y los carga en ClickHouse. Al finalizar, ejecuta una validacion con SQL para comprobar que la cantidad de registros cargados coincida con el dataset original.

## Estructura general

```text
.
|-- docker-compose.yml
|-- requirements.txt
|-- README.md
|-- scripts/
|   |-- 01_http_to_bucket.py
|   |-- 02_parquet_to_iceberg.py
|   |-- 03_minio_to_azure.py
|   |-- 04_minio_to_clickhouse.py
|-- src/
```

Los datos descargados, evidencias, archivos temporales y credenciales locales no hacen parte del codigo fuente principal y no deberian subirse al repositorio.

## Ejecucion general

Primero se levantan los servicios:

```bash
docker compose up -d
```

Luego se ejecutan los scripts en orden:

```bash
python scripts/01_http_to_bucket.py
python scripts/02_parquet_to_iceberg.py
python scripts/03_minio_to_azure.py
python scripts/04_minio_to_clickhouse.py
```

Tambien pueden ejecutarse desde una terminal de Jupyter, dependiendo del ambiente configurado.

## Validacion

El proyecto se valida revisando que:

- Los servicios de Docker esten activos.
- MinIO contenga los datos iniciales y los archivos generados por Iceberg.
- Nessie muestre el namespace y la tabla registrada.
- Azure Storage reciba los datos en la ubicacion configurada.
- ClickHouse tenga la tabla final cargada.
- La consulta de conteo en ClickHouse coincida con el numero de registros del dataset original.

Consulta de validacion esperada en ClickHouse:

```sql
SELECT count(*) FROM tabla_final;
```

## Seguridad

Este repositorio no debe incluir:

- Claves de Azure.
- Tokens.
- Archivos de secretos.
- Datos descargados.
- Evidencias locales pesadas.
- Archivos generados automaticamente por Python o Jupyter.

La configuracion sensible debe manejarse localmente mediante archivos privados o variables de entorno.

## Resultado

Al finalizar el pipeline, se obtiene un flujo completo de datos que parte desde una fuente HTTP, pasa por un data lake con MinIO, se organiza como tabla Iceberg registrada en Nessie, se replica hacia Azure Storage y queda disponible en ClickHouse para analisis mediante SQL.
