# Orquestación Transfermarkt Silver

## Alcance y componentes verificados

`football.transfermarkt.transfermarkt_silver` coordina únicamente las
transformaciones Silver. No descarga fuentes, no modifica la lógica de ingesta
Bronze y no contiene SQL de negocio: ese SQL y sus pruebas permanecen en dbt.

El entorno inspeccionado usa Kestra `2.0.4`,
`plugin-script-python:1.13.0` con Process runner y
`plugin-docker:1.6.2`. El flow ejecuta la imagen local fijada
`transfermarkt-dbt:1.12.5-snowflake-1.12.1` mediante
`io.kestra.plugin.docker.cli.Run`. El socket Docker se monta solo en Kestra;
por ello ese servicio debe considerarse operador de confianza del daemon.

## Encadenamiento único

Bronze conserva su único Schedule `0 6 1 * *`, zona
`America/Guayaquil`, con `recoverMissedSchedules: NONE` y desactivado hasta que
su preflight sea validado. No existe Schedule Silver adicional.

Después de `finalize` y de la barrera `fail_incomplete_batch`, Bronze llama al
flow Silver mediante un único Subflow con `wait: true` y
`transmitFailed: true`. También lo llama cuando todos los assets resultan
`SKIPPED_UNCHANGED`: Silver decide con su propio checkpoint, no con la bandera
de cambios de ese batch. Un fallo Bronze impide la llamada; un fallo Silver se
propaga sin convertir el batch Bronze ya confirmado en un checkpoint Silver.

Silver sigue siendo ejecutable manualmente con:

```powershell
# Normal; target vacío usa DBT_TARGET (dev por defecto).
.\scripts\run-silver-flow.ps1

# Reprocesamiento explícito en el target aislado.
.\scripts\run-silver-flow.ps1 -Target test -Force

# Backfill solo con un manifiesto completo de versiones Bronze conservadas.
.\scripts\run-silver-flow.ps1 -Mode backfill -Target test `
  -ManifestPath .\manifest-preservado.json
```

Repetir una fecha mensual pasada no crea un snapshot histórico. El modo
`backfill` exige los 12 pares reales `ingestion_run_id`/SHA-256 y comprueba que
sean `SUCCESS` y que sus filas sigan en RAW. Sus timestamps de captura reales
se conservan; no se reemplazan con fechas de partidos ni con la fecha lógica
del Schedule.

## Preflight, manifiesto y decisión

La tarea `plan`:

1. usa el conector Python central con password y `authenticator="snowflake"`;
2. ejecuta `SELECT 1`, selecciona rol/warehouse/database y comprueba permisos
   para los schemas efectivos de staging/Silver;
3. resuelve un `SUCCESS` por cada una de las 12 fuentes o valida el manifiesto
   fijo solicitado;
4. reconcilia cada versión contra sus filas RAW por run ID y SHA-256;
5. crea una copia inmutable del proyecto dbt para esa ejecución;
6. calcula SHA-256 sobre rutas y contenido de SQL, YAML, macros, seeds, tests y
   configuración dbt. Por tanto detecta cambios locales aunque no haya commit;
7. compara manifiesto y transformación con el último checkpoint validado.

Se ejecuta dbt si `force=true`, falta una salida final, no hay checkpoint
Silver exitoso, hay versiones Bronze pendientes o cambió el fingerprint. Solo
se omite cuando las cuatro tablas existen y ambos fingerprints ya fueron
validados. El flow usa `concurrency.limit: 1` con `QUEUE`; además, el preflight
rechaza otro run `RUNNING` reciente del mismo target. Como Bronze es append-only,
una ingesta concurrente no cambia las filas del manifiesto fijado.

## Build, calidad y reintentos

El comando ejecutado en la copia inmutable es conceptualmente:

```text
dbt build --target <target> --select +tag:transfermarkt_silver \
  --vars <manifiesto fijo> --no-version-check --no-partial-parse
```

El tag común cubre staging, seeds/aliases, controles, clasificadores, bases,
quarantine, reconciliación, resoluciones y los cuatro modelos finales con sus
pruebas. Warnings de relaciones con cobertura histórica incompleta se aceptan
y contabilizan; un test crítico `fail/error`, un modelo final ausente o un exit
code dbt distinto de cero impide el checkpoint.

Hay como máximo tres intentos totales y solo cuando la salida coincide con un
error transitorio reconocido de red/servicio Snowflake. Autenticación/MFA,
permisos, schema, compilación y calidad se clasifican permanentes y no reciben
retry genérico. Repetir es idempotente para las vistas/tablas reproducibles,
pero dbt no ofrece rollback global: tras un fallo pueden quedar objetos
materializados y no quedan validados para consumidores posteriores.

## Auditoría, checkpoint y artefactos

Cada target guarda en su schema Silver efectivo:

- `TRANSFERMARKT_SILVER_RUNS`: ejecución, modo, target, force, razones,
  manifiesto, fingerprints, tiempos, estado, resumen dbt, conteos y error;
- `TRANSFERMARKT_SILVER_RUN_SOURCES`: una fila por fuente con run, versión,
  checksum, archivo, captura/check real, fin Bronze y filas auditadas;
- `TRANSFERMARKT_SILVER_CHECKPOINT`: solo la última ejecución completamente
  construida y validada por target.

Kestra conserva `silver_plan.json`, la copia dbt de entrada,
`manifest.json`, `run_results.json`, logs/metadata por intento,
`silver_summary.json` y `quarantine_evidence.json` cuando están disponibles.
La tarea dbt primero persiste evidencia; después `finalize` decide éxito o
fallo. El checkpoint se reemplaza únicamente después de validar artifacts,
los cuatro modelos y sus conteos. Un error inesperado marca el run `FAILED` y
la siguiente revisión mensual puede recuperar el trabajo aunque Bronze no
tenga archivos nuevos.

Las credenciales se inyectan desde Compose. `ENV_*` es el alias requerido por
el namespace Pebble `envs` de Kestra; nunca se escribe su valor en YAML,
artefactos o argumentos. Los logs se redactan defensivamente. No se montan ni
leen claves privadas.

La auditoría reciente puede consultarse desde el contenedor, sin imprimir
credenciales:

```powershell
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python `
  -m ingestion.silver audit --target test --limit 10
```

La frescura de una captura es independiente de las fechas de negocio. Un mes
sin novedad puede producir un Bronze correcto y un Silver omitido correctamente;
no se etiqueta como fallo de frescura por ese solo hecho.
