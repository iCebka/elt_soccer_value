# Arquitectura de ingesta Bronze

```mermaid
flowchart LR
    M[Manual / Schedule mensual / backfill finito]
    K[Kestra 2.0.4<br/>concurrency limit 1]
    H[HEAD condicional<br/>ETag opaco]
    C[(Auditoría Snowflake<br/>última versión exitosa)]
    G[GET condicional<br/>stream + SHA-256]
    O[@STAGE<br/>original/asset/sha]
    T[(ASSET_RAW)]
    V[ASSET_LATEST]
    PG[(PostgreSQL<br/>estado Kestra)]

    M --> K --> H
    H <--> C
    H -->|igual o 304| C
    H -->|primera/cambió/sin ETag| G
    G -->|304| C
    G -->|checksum conocido| C
    G -->|versión nueva| O --> T --> V
    T --> C
    K <--> PG
```

## Consulta y publicación

Todos los caminos usan el mismo flujo y empiezan consultando la fuente. La identidad de carga es `Dataset1` + asset/tabla + URL + ETag/checksum; ni el `executionId` ni el mes lógico identifican contenido.

HEAD usa `If-None-Match` cuando existe un checkpoint exitoso. El ETag se guarda sin quitar comillas ni transformar prefijos débiles. `304` o igualdad exacta permiten `SKIPPED_UNCHANGED`. Si el servidor no ofrece un ETag fiable, tamaño y `Last-Modified` quedan auditados pero no se usan para saltar: GET y SHA-256 deciden. La versión de la respuesta GET prevalece para detectar cambios ocurridos entre HEAD y GET.

Una descarga nueva se conserva en `original/<asset>/<sha256>/`; NDJSON queda en `prepared/<asset>/<sha256>/<run_id>/`. La carga usa una tabla temporal. El borrado de residuos de esa URL/checksum, la inserción RAW y el checkpoint `SUCCESS` ocurren en una transacción. Esto preserva duplicados existentes dentro del CSV, pero evita duplicar un snapshot al recuperar un fallo.

Las vistas `*_LATEST` continúan apuntando únicamente al intento `SUCCESS` más reciente. `SKIPPED_UNCHANGED` referencia una versión ya publicada, pero no la sustituye como dueño de filas RAW.

## Tiempo y backfill

- `LOGICAL_DATE`: fecha que Kestra programa o recupera.
- `STARTED_AT`: inicio efectivo de ejecución.
- `SOURCE_CHECKED_AT`: instante real del HEAD/GET.
- `CAPTURED_AT`: instante de descarga de la versión publicada o referenciada.
- fechas dentro de `RAW_RECORD`: fechas de negocio propias de cada dataset.

Un backfill pasado consulta el snapshot actual. No crea nombres mensuales, no filtra datasets completos por mes y no reconstruye un point-in-time que el proveedor no publica. `recoverMissedSchedules: NONE` evita catch-up automático tras reiniciar Docker; la recuperación se solicita con inicio y fin explícitos.

## Reintentos

El cliente HTTP dispone de tres solicitudes totales por asset para HEAD + GET. Solo timeout/conexión, `429` y `5xx` consumen retries, con 10 s iniciales, factor 2, máximo 120 s y `Retry-After`. Errores HTTP permanentes, URL inválida y datos incompatibles fallan inmediatamente.

Kestra no aplica un retry ciego al comando, porque no distingue el tipo de excepción y multiplicaría intentos. El conector Snowflake repite hasta tres veces la conexión/publicación solo ante `OperationalError` transitorio y reutiliza el archivo ya capturado. Autenticación/MFA, privilegios, objetos ausentes y SQL inválido no se reintentan. Los errores `002043` incluyen contexto de sentencia y objeto.

## Seguridad

Kestra usa Basic Auth y el conector Snowflake recibe `account`, `user`, `password`, `role`, `warehouse` y `database` desde `.env`. El autenticador es `snowflake`; no existe ruta ni montaje `.p8`. El Process runner hereda el entorno del servicio Kestra, y la salida de errores redacta contraseñas/tokens configurados.
