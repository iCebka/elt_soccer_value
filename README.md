# Transfermarkt datasets → Snowflake Bronze

Módulo reproducible de ingesta de los 12 snapshots CSV publicados por [`dcaribou/transfermarkt-datasets`](https://github.com/dcaribou/transfermarkt-datasets). Kestra orquesta la descarga y Snowflake conserva el `.csv.gz` original, las versiones RAW y los controles auditables. La transformación Silver, dbt, Spark, lesiones y modelos predictivos están fuera de este alcance.

La fuente es batch: publica snapshots completos, no una API de eventos incrementales. Aquí `incremental` significa “cargar solo si cambió el SHA-256”; `backfill` carga todo el historial que el snapshot actual contiene y retoma fallos, pero no recrea cómo se veía el proveedor en fechas pasadas.

## Componentes

- Kestra `2.0.4` con plugin Python `1.13.0`, Process runner y Basic Auth.
- PostgreSQL `16.10` como backend de Kestra, no como almacén de fútbol.
- Python en una imagen personalizada; descarga streaming, validación gzip/CSV, SHA-256, NDJSON por bloques y Snowflake Connector `4.8.0`.
- Snowflake con stage interno, una tabla RAW y una vista `LATEST` por asset, además de dos tablas de control.
- Trigger semanal lunes 03:00 `America/Guayaquil`, entregado desactivado.

Vea [arquitectura](docs/architecture.md), [contrato Bronze](docs/bronze_contract.md) y [verificaciones](docs/verification.md).

## 1. Requisitos

- Docker Desktop con `docker compose`.
- Una cuenta Snowflake y un rol con `USAGE` sobre el warehouse/database, `CREATE SCHEMA` en el database si `BRONZE` aún no existe, y privilegios de creación/lectura/escritura en ese schema.
- Autenticación no interactiva con usuario y contraseña; la política de la cuenta debe permitirla para el usuario técnico.

El pipeline no crea el database ni el warehouse. Los valores propuestos son database `FOOTBALL`, schema `BRONZE` y un warehouse dedicado como `INGEST_WH`.

## 2. Configuración (PowerShell)

```powershell
Copy-Item .env.example .env
notepad .env
```

Complete en `.env` al menos:

- `POSTGRES_PASSWORD`, `KESTRA_USERNAME`, `KESTRA_PASSWORD`.
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`.
- `SNOWFLAKE_DATABASE` y `SNOWFLAKE_BRONZE_SCHEMA` si no usará `FOOTBALL.BRONZE`.

Use `SNOWFLAKE_AUTH_METHOD=password`. El pipeline pasa `authenticator="snowflake"` al conector; no admite claves privadas ni autenticación interactiva. `.env` y los archivos de trabajo están ignorados por Git. No escriba la contraseña en comandos, flows, commits ni mensajes de soporte.

`SNOWFLAKE_ACCOUNT` debe ser el identificador de cuenta, preferiblemente `organizacion-cuenta`, sin `https://` ni el sufijo `.snowflakecomputing.com`; no es el nombre de usuario. Un account locator también es válido, pero fuera de AWS us-west requiere los segmentos adicionales de región/proveedor. Consulte la [configuración oficial de clientes Snowflake](https://docs.snowflake.com/en/user-guide/gen-conn-config).

| Variable del repositorio | Parámetro del conector/destino |
|---|---|
| `SNOWFLAKE_ACCOUNT` | `account` |
| `SNOWFLAKE_USER` | `user` |
| `SNOWFLAKE_PASSWORD` | `password` |
| `SNOWFLAKE_ROLE` | `role` |
| `SNOWFLAKE_WAREHOUSE` | `warehouse` |
| `SNOWFLAKE_DATABASE` | `database` |
| `SNOWFLAKE_BRONZE_SCHEMA` | schema Bronze usado por el pipeline |

Si la cuenta exige MFA para todas las autenticaciones por contraseña, el preflight fallará con el mensaje de Snowflake. No desactive MFA ni use `externalbrowser` para el job programado: aplique una política de autenticación aprobada para un usuario de servicio o el método no interactivo permitido por su organización.

## 3. Levantar Kestra y registrar flows

```powershell
# Valida Compose sin imprimir la configuración expandida ni secretos.
docker compose config --quiet

docker compose build
docker compose up -d --force-recreate kestra
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\wait-kestra.ps1
.\scripts\register-flows.ps1
```

Abra `http://localhost:8080` (o el `KESTRA_PORT` configurado) y use las credenciales Basic Auth de `.env`. `register-flows.ps1` valida y hace upsert sin borrar flows ajenos al directorio. Los comandos Python usan el runner `Process`, por lo que heredan las variables del servicio Kestra; no se crean contenedores de tarea separados.

## 4. Ejecutar

Carga completa de `competitions`, útil como smoke test real:

```powershell
.\scripts\run-flow.ps1 -Mode incremental -Assets competitions
```

Segunda ejecución idéntica (debe quedar `SKIPPED`, sin filas RAW duplicadas):

```powershell
.\scripts\run-flow.ps1 -Mode incremental -Assets competitions
```

Todos los assets:

```powershell
$allAssets = @(
  'competitions', 'clubs', 'players', 'games', 'appearances',
  'player_valuations', 'transfers', 'club_games', 'game_events',
  'game_lineups', 'countries', 'national_teams'
)
.\scripts\run-flow.ps1 -Mode incremental -Assets $allAssets
```

Backfill del historial disponible dentro de los snapshots seleccionados:

```powershell
.\scripts\run-flow.ps1 -Mode backfill -Assets games,appearances,player_valuations
```

El script devuelve inmediatamente el JSON de la ejecución. Siga su estado en la UI. El flow principal espera los subflows, genera `summary.json`, registra el lote y falla si cualquier asset requerido falla.

## 5. Consultar Snowflake

```sql
-- Resultado de lotes recientes.
SELECT BATCH_ID, MODE, STATUS, STARTED_AT, FINISHED_AT, SUMMARY, ERROR
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_BATCHES
ORDER BY STARTED_AT DESC;

-- Auditoría por archivo/versión.
SELECT ASSET, SOURCE_FILE_SHA256, STATUS, ROWS_READ, ROWS_LOADED,
       OBSERVED_HEADERS, SCHEMA_CHANGED, ORIGINAL_STAGE_PATH
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_FILES
ORDER BY STARTED_AT DESC;

-- Una fila por registro del CSV de la versión exitosa más reciente.
SELECT COUNT(*) FROM FOOTBALL.BRONZE.COMPETITIONS_LATEST;

-- Originales archivados y archivos preparados.
LIST @FOOTBALL.BRONZE.TRANSFERMARKT_BRONZE_STAGE/original/competitions/;
LIST @FOOTBALL.BRONZE.TRANSFERMARKT_BRONZE_STAGE/prepared/competitions/;
```

Los objetos se crean de forma idempotente con el comando `check` del flow. El DDL de referencia está en [`sql/bronze/objects.sql`](sql/bronze/objects.sql).

## 6. Pruebas locales

Las pruebas usan fixtures sintéticos identificados como tales; no los registran como cargas reales:

```powershell
docker build --target test -t transfermarkt-bronze-tests:local .
docker run --rm transfermarkt-bronze-tests:local
```

Cubren comas, comillas, saltos de línea, Unicode, campos vacíos, deriva de encabezados, gzip inválido, segunda ejecución idempotente, recuperación tras un fallo de publicación simulado y autenticación por contraseña sin depender de una clave privada.

Validación completa de un CSV real sin declararlo cargado en Bronze:

```powershell
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml inspect-source --asset competitions
```

Comandos de diagnóstico:

```powershell
docker compose ps
docker compose logs --tail 200 kestra
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml check
```

El último comando ejecuta `SELECT 1`, crea o verifica los objetos Bronze y requiere credenciales Snowflake válidas. Los mensajes de la aplicación redactan la contraseña si una excepción llegara a contenerla.

## 7. Semántica operativa

- El SHA-256 se calcula sobre los bytes del `.csv.gz` original. Un checksum ya `SUCCESS` produce `SKIPPED`; un contenido distinto es una nueva versión.
- El original queda en `original/<asset>/<sha256>/`; los NDJSON de carga quedan separados en `prepared/<asset>/<sha256>/<run_id>/`.
- `COPY INTO` carga una temporal. RAW y el control `SUCCESS` se publican en la misma transacción. Ante fallo se hace rollback y el intento queda `FAILED`.
- Las vistas `*_LATEST` eligen solo la versión `SUCCESS` más reciente. Su grain es una fila original del CSV dentro de ese snapshot.
- Los perfiles como `players` representan estado reciente; los datasets de eventos/valoraciones contienen el historial incluido por el snapshot, no snapshots históricos del proveedor.
- Los IDs de origen permanecen textuales dentro de `RAW_RECORD` para joins posteriores.

## Cobertura observada de la fuente

El 2 de octubre de 2026 se comprobaron directamente las 12 URLs de `config/assets.yml`: todas respondieron `200`, eran gzip legible y sus encabezados coincidían con el catálogo. El README oficial mantiene las actualizaciones pausadas e informa: `games` hasta 2026-07-06, `appearances` hasta 2026-06-28 y `player_valuations` hasta 2026-06-12. Descargar hoy no vuelve actuales los eventos ni prueba que una fila estaba disponible en una fecha histórica.
