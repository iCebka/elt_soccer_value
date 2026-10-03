# Arquitectura de ingesta Bronze

```mermaid
flowchart LR
    S[CSV .csv.gz publicados<br/>dcaribou/transfermarkt-datasets]
    K[Kestra 2.0.4<br/>flow principal]
    A[Subflow por asset<br/>Process runner]
    P[Python 3<br/>stream + SHA-256 + csv iterativo]
    O[@TRANSFERMARKT_BRONZE_STAGE<br/>original/asset/sha]
    J[@TRANSFERMARKT_BRONZE_STAGE<br/>prepared/asset/sha/run]
    T[(ASSET_RAW)]
    C[(tablas de control)]
    V[ASSET_LATEST]
    PG[(PostgreSQL 16.10<br/>estado de Kestra)]

    S --> A
    K --> A --> P
    P --> O
    P --> J -->|COPY INTO temporal| T
    P --> C
    C --> V
    T --> V
    K <--> PG
```

## Decisiones

La fuente se procesa por lotes porque publica snapshots CSV completos y no ofrece una API de cambios ni archivos por temporada. En modo `incremental`, el pipeline compara el SHA-256 del snapshot completo con versiones exitosas; en `backfill` recupera de la misma forma todo el histórico presente en el snapshot disponible. Ninguno de los dos modos inventa snapshots mensuales pasados.

Cada descarga se escribe por streaming en el almacenamiento temporal del task, se calcula el SHA-256 sobre el `.csv.gz` original y se recorre el gzip completo con el parser CSV. El parser conserva todos los valores como texto, incluidas cadenas vacías, y respeta comas, comillas y saltos de línea embebidos. Los encabezados se comparan con `config/assets.yml`: cualquier cambio se registra en `OBSERVED_HEADERS` y `SCHEMA_CHANGED`; columnas nuevas permanecen dentro de `RAW_RECORD`.

El original se archiva sin recomprimir en `original/<asset>/<sha256>/`. Los registros preparados se escriben como NDJSON en bloques y se cargan con `PUT` + `COPY INTO` a una tabla temporal. La publicación borra cualquier residuo incompleto del mismo checksum, inserta RAW y marca el control `SUCCESS` dentro de una sola transacción. Si falla, la transacción se revierte y el intento queda `FAILED`; una repetición puede retomarlo. Kestra limita a una ejecución concurrente tanto del lote como del subflow para evitar carreras sobre el mismo destino.

PostgreSQL guarda exclusivamente el estado de Kestra. Los datos de fútbol, archivos y controles de ingesta quedan en Snowflake.

## Reintentos y errores

- HTTP `429`, timeouts, errores de conexión y `5xx`: hasta cuatro intentos con espera acotada; `Retry-After` se respeta en `429`.
- HTTP `404`/`410`, otros `4xx`, URL que no termina en `.csv.gz`, gzip/CSV inválido y encabezados duplicados: fallo permanente inmediato.
- Conexión transitoria a Snowflake: hasta tres intentos. Errores de autenticación/configuración se devuelven de forma clara por el conector y nunca se imprimen credenciales.
- Los subflows pueden terminar individualmente en fallo para que el coordinador procese los demás. La tarea final siempre compone el resumen y luego hace fallar el lote si faltó cualquier asset solicitado.

## Seguridad y autenticación

La configuración selecciona explícitamente usuario, rol, warehouse, database y schema. El pipeline usa contraseña con el autenticador estándar `snowflake`; no lee ni monta claves privadas. `externalbrowser` no se usa porque requiere interacción humana. Kestra expone Basic Auth y las credenciales se leen desde `.env`, que está ignorado por Git. El runner `Process` hereda ese entorno dentro del mismo contenedor Kestra.

## Reconstrucción desde originales

Para reconstruir Bronze sin volver a consultar al proveedor, descargue con `GET` los objetos de `@TRANSFERMARKT_BRONZE_STAGE/original/<asset>/<sha256>/`, ejecute el mismo conversor `prepare_ndjson` y publique con un nuevo `run_id`. El checksum y los encabezados están en `TRANSFERMARKT_INGESTION_FILES`. Esta operación deliberadamente no sobrescribe otras versiones exitosas ni afirma cuál era el estado del proveedor antes de la fecha del archivo disponible.

## Estado de la fuente observado

Verificado el 2 de octubre de 2026: las 12 URLs configuradas respondieron HTTP 200 y sus streams gzip entregaron los encabezados documentados. El README oficial indica actualizaciones pausadas: `games` hasta 2026-07-06, `appearances` hasta 2026-06-28 y `player_valuations` hasta 2026-06-12; las plantillas 2026/27 no están cubiertas. La fecha de descarga es metadata de ingesta, no fecha del evento ni garantía de disponibilidad histórica.

Referencias verificadas: [README del proveedor](https://github.com/dcaribou/transfermarkt-datasets), [Docker Compose de Kestra](https://kestra.io/docs/installation/docker-compose), [subflows de Kestra](https://kestra.io/docs/workflow-components/subflows), [carga COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table) y [conexión del conector Python de Snowflake](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect).

