# Transfermarkt datasets → Snowflake Bronze

Ingesta Source → Bronze de los 12 snapshots CSV de [`dcaribou/transfermarkt-datasets`](https://github.com/dcaribou/transfermarkt-datasets). Kestra `2.0.4` orquesta; PostgreSQL conserva su estado; Python consulta la fuente; Snowflake archiva cada versión nueva y publica tablas RAW. dbt selecciona versiones Bronze auditadas, construye staging y entidades base Silver limpias, y publica los cuatro productos Silver contratados: `silver_players_current`, `silver_games_enriched`, `silver_player_match` y `silver_player_valuations_enriched`. Gold y el modelo predictivo se implementan en etapas posteriores.

La fuente publica snapshots completos, no archivos mensuales. `incremental` significa “consultar la fuente y cargar solo si el contenido cambió”. Un backfill de septiembre ejecutado hoy consulta el snapshot disponible hoy: no reconstruye cómo era el archivo en septiembre.

## Componentes y objetos

- Kestra `2.0.4`, plugin Python `1.13.0`, Process runner, plugin Docker
  `1.6.2` para el contenedor dbt y Basic Auth.
- Schedule `monthly_first_day`: `0 6 1 * *`, `America/Guayaquil`, inicialmente desactivado.
- El “retrigger mensual” es ese Schedule creando una ejecución nueva; no es el retry de una ejecución antigua.
- Una sola ejecución simultánea del flujo Bronze; manual, Schedule y backfill comparten el mismo flujo.
- HEAD condicional por asset y GET solo cuando hace falta. ETag se conserva como valor opaco, incluidas comillas o prefijo débil.
- 12 tablas `*_RAW`, 12 vistas `*_LATEST`, dos tablas de auditoría, un stage y un file format: 28 objetos de datos/control más el schema.
- Autenticación Snowflake exclusivamente con usuario/contraseña; no se leen ni montan claves `.p8`.

Vea [arquitectura](docs/architecture.md), [contrato Bronze](docs/bronze_contract.md) y [verificaciones](docs/verification.md).

## Infraestructura dbt para Silver

El servicio `dbt` es de una sola ejecución y está aislado detrás del profile Compose `silver`; no altera el arranque normal de Bronze/Kestra. Usa `dbt-core==1.12.5`, `dbt-snowflake==1.12.1`, autenticación por contraseña y el mismo account/rol/warehouse/database de la ingesta.

```powershell
docker compose --profile silver build dbt
docker compose --profile silver run --rm dbt parse --no-version-check
docker compose --profile silver run --rm dbt run-operation report_effective_schemas --target dev --no-version-check
docker compose --profile silver run --rm dbt debug --target dev --no-version-check
docker compose --profile silver run --rm dbt build --target test --select +tag:silver_stage_2 --no-version-check
docker compose --profile silver run --rm dbt build --target test --select +tag:silver_stage_3 --no-version-check
docker compose --profile silver run --rm dbt build --target test --select +tag:silver_stage_4 --no-version-check
docker compose --profile silver run --rm dbt build --target test --select +tag:silver_stage_5 --no-version-check
docker compose --profile silver run --rm dbt build --target test --select +tag:silver_stage_6 --no-version-check
```

`dev` es el target seguro predeterminado: genera `DBT_DEV_STAGING` y `DBT_DEV_SILVER`; `test` usa `DBT_TEST_STAGING` y `DBT_TEST_SILVER`. `prod` genera exactamente `STAGING` y `SILVER` (o los nombres configurados), sin prefijo. El build de etapa 2 selecciona una versión `SUCCESS` por fuente en un manifiesto materializado, lee RAW sin mezclar snapshots y ejecuta seeds, bases, quarantine, reconciliación y pruebas. Las etapas 3–6 construyen respectivamente perfil actual, partidos, appearances y el historial completo de valoraciones. `dbt debug` sí abre una conexión; `dbt parse` y `report_effective_schemas` solo validan configuración/nombres. Consulte el [contrato consolidado](docs/silver_contract.md), [contrato base](docs/silver_transfermarkt.md), [contrato de jugadores](docs/silver_players_current.md), [contrato de partidos](docs/silver_games_enriched.md), [contrato player-match](docs/silver_player_match.md), [contrato de valoraciones](docs/silver_player_valuations_enriched.md) y [progreso](docs/silver_implementation_progress.md).

## Orquestación Silver

El flow independiente `football.transfermarkt.transfermarkt_silver` ejecuta
`dbt build --select +tag:transfermarkt_silver` en la imagen dbt fijada. Bronze
lo llama como único Subflow después de toda comprobación exitosa, incluso si
no hubo archivos nuevos; Silver también admite ejecución manual normal,
`force` y backfill con un manifiesto explícito de versiones conservadas.

```powershell
.\scripts\run-silver-flow.ps1 -Target test
.\scripts\run-silver-flow.ps1 -Target test -Force
```

No se agregó otro Schedule: se conserva el mensual de Bronze. La decisión usa
versiones Bronze, existencia de las cuatro salidas y un fingerprint por
contenido de SQL/YAML/macros/seeds/configuración. Auditoría, checkpoint,
artefactos, reintentos clasificados y la limitación no atómica de dbt se
detallan en [orquestación Silver](docs/silver_orchestration.md).

### Entrega y aceptación de Silver

Los cuatro productos finales están cerrados con estos grains:

| Tabla | Grain |
|---|---|
| `silver_players_current` | una fila por `player_id` |
| `silver_games_enriched` | una fila por `game_id` |
| `silver_player_match` | una fila por `appearance_id` |
| `silver_player_valuations_enriched` | una fila por (`player_id`, `valuation_date`) |

El siguiente procedimiento fue comprobado en Windows/PowerShell sobre el
target aislado `test`. Recrea solo Kestra y conserva los volúmenes:

```powershell
docker compose config --quiet
docker compose --profile silver build dbt
docker compose build kestra
docker compose up -d postgres
docker compose up -d --no-deps --force-recreate kestra

powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\wait-kestra.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\register-flows.ps1

docker compose --profile silver run --rm dbt parse `
  --target test --no-version-check --no-partial-parse --warn-error
docker compose --profile silver run --rm dbt debug `
  --target test --no-version-check
docker compose --profile silver run --rm dbt show `
  --target test --inline "select 1 as connection_ok" --no-version-check

powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run-silver-flow.ps1 -Target test
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run-silver-flow.ps1 -Target test -Force
```

`dbt debug` muestra identificadores de conexión, aunque no imprime la
contraseña; no publique esa salida. El flujo real es
`football.transfermarkt.transfermarkt_silver` y ejecuta el contenedor dbt con
las variables de Compose. Consultas operativas, sin incluir secretos:

```powershell
# Runs/checkpoint/versiones Bronze usadas; límite 1..100.
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python `
  -m ingestion.silver audit --target test --limit 10

# Grains, huellas de negocio, cobertura, importes, reconciliación y quarantine.
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python `
  -m ingestion.silver verify --target test

# Estado de una ejecución devuelta por run-silver-flow.ps1.
$executionId = '<execution-id>'
docker compose exec -T -e "SILVER_EXECUTION_ID=$executionId" kestra sh -lc `
  'curl --fail --silent --user "$KESTRA_BASIC_AUTH_USERNAME:$KESTRA_BASIC_AUTH_PASSWORD" "http://localhost:8080/api/v1/main/executions/$SILVER_EXECUTION_ID"'
```

Los artifacts `manifest.json` y `run_results.json` quedan en la ejecución de
Kestra y sus hashes/rutas quedan auditados. Una ejecución normal posterior se
omite solo con razón `already_validated`; `-Force` reconstruye de forma
idempotente. El Schedule Bronze permanece desactivado hasta un preflight
operativo explícito, y no se creó un Schedule Silver adicional.

## 1. Requisitos y configuración en PowerShell

Necesita Docker Desktop con Compose y un rol Snowflake con acceso al warehouse/database y privilegios para crear/usar `BRONZE`. El pipeline no crea el database ni el warehouse.

```powershell
Copy-Item .env.example .env
notepad .env
```

Complete localmente los secretos; nunca los pegue en el chat ni los registre en Git. Variables finales:

| Variable | Uso |
|---|---|
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | Estado interno de Kestra |
| `KESTRA_PORT`, `KESTRA_USERNAME`, `KESTRA_PASSWORD` | UI/API con Basic Auth |
| `SNOWFLAKE_ACCOUNT` | `account`, preferentemente `organización-cuenta`, sin URL |
| `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` | `user` y `password` |
| `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` | rol y warehouse del loader |
| `SNOWFLAKE_DATABASE`, `SNOWFLAKE_BRONZE_SCHEMA` | destino, por defecto `FOOTBALL.BRONZE` |
| `SNOWFLAKE_AUTH_METHOD=password` | único modo aceptado |
| `DBT_TARGET` | target manual predeterminado: `dev`, `test` o `prod` |
| `DBT_DEV_SCHEMA`, `DBT_TEST_SCHEMA` | prefijos aislados para targets no productivos |
| `SNOWFLAKE_STAGING_SCHEMA`, `SNOWFLAKE_SILVER_SCHEMA` | schemas exactos usados solo por `prod` |
| `DBT_IMAGE` | imagen fijada que ejecuta el task Docker de Kestra |

El conector usa `authenticator="snowflake"`; no usa `authenticator="password"`, claves privadas ni `externalbrowser`. Si Snowflake exige MFA/políticas incompatibles con contraseña no interactiva, el preflight falla y debe configurarse un usuario de servicio conforme a la política; no desactive MFA para sortearla.

## 2. Construir, recrear y registrar los flows

```powershell
# No imprime la configuración expandida ni secretos.
docker compose config --quiet

docker compose build
docker compose up -d postgres
docker compose up -d --no-deps --force-recreate kestra
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\wait-kestra.ps1
.\scripts\register-flows.ps1
```

Abra `http://localhost:8080` y use las credenciales Kestra de `.env`. El Process runner ejecuta dentro del contenedor Kestra y hereda las variables Snowflake. `recoverMissedSchedules: NONE` está fijado en el trigger y globalmente: un reinicio de Docker no dispara un histórico ilimitado; los huecos se recuperan solo mediante un backfill finito.

## 3. Verificar el preflight y activar el Schedule

Antes de activar el Schedule:

```powershell
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml check
```

El comando ejecuta `SELECT 1` y crea/migra objetos sin borrar datos. Solo después de verlo terminar correctamente active el trigger:

```powershell
.\scripts\manage-schedule.ps1 -Action Enable
```

Pausar futuras ejecuciones mensuales:

```powershell
.\scripts\manage-schedule.ps1 -Action Disable
```

También puede usar el toggle Enabled en la pestaña **Triggers** del flow. Registrar otra vez el YAML vuelve a aplicar `disabled: true`; active después del registro y preflight.

## 4. Ejecución manual y relanzamiento

Smoke test pequeño:

```powershell
.\scripts\run-flow.ps1 -Mode incremental -Assets competitions
```

Repita el mismo comando para relanzar desde el inicio. Cada nueva ejecución vuelve a hacer HEAD y, si corresponde, GET; no reutiliza una descarga de una ejecución anterior. Si el ETag no cambió debe registrar `SKIPPED_UNCHANGED` y no descargar ni cargar.

Todos los assets:

```powershell
$allAssets = @(
  'competitions', 'clubs', 'players', 'games', 'appearances',
  'player_valuations', 'transfers', 'club_games', 'game_events',
  'game_lineups', 'countries', 'national_teams'
)
.\scripts\run-flow.ps1 -Mode incremental -Assets $allAssets
```

En la UI use **Execute** con `assets` y `mode=incremental`. Reiniciar/replay de una ejecución fallida es posible, pero para buscar novedades se recomienda una ejecución nueva desde el inicio, de modo que vuelva a consultar la fuente.

## 5. Backfill mensual finito

El trigger debe estar habilitado mientras Kestra procesa el backfill. Primero haga una vista previa local; no llama a la API:

```powershell
.\scripts\manage-schedule.ps1 -Action Preview `
  -Start '2026-07-01T00:00:00-05:00' `
  -End   '2026-09-02T00:00:00-05:00'
```

Crear exactamente ese intervalo:

```powershell
.\scripts\manage-schedule.ps1 -Action Enable
.\scripts\manage-schedule.ps1 -Action Create `
  -Start '2026-07-01T00:00:00-05:00' `
  -End   '2026-09-02T00:00:00-05:00'
```

Pausar, reanudar o cancelar únicamente las ejecuciones pendientes del backfill:

```powershell
.\scripts\manage-schedule.ps1 -Action Pause
.\scripts\manage-schedule.ps1 -Action Resume
.\scripts\manage-schedule.ps1 -Action Cancel
```

`Cancel` elimina el backfill y evita nuevas ejecuciones recuperadas; no mata una ejecución que ya está corriendo ni borra datos. Si también quiere detener el cron futuro, use `-Action Disable`. El script exige inicio y fin RFC3339, muestra cada fecha lógica antes de enviar el `PUT /api/v1/main/triggers` y pasa `mode=backfill`.

Cada ejecución recuperada conserva por separado:

- `LOGICAL_DATE`: mes/instante solicitado por el Schedule.
- `STARTED_AT` y `SOURCE_CHECKED_AT`: ejecución y consulta efectiva.
- `CAPTURED_AT`: momento en que se descargaron los bytes de esa versión.
- `SOURCE_VERSION`, `REMOTE_ETAG`, `DOWNLOAD_ETAG` y SHA-256: identidad observada.

Por tanto, varias fechas lógicas pueden quedar `SKIPPED_UNCHANGED` contra la misma versión actual. Esto es correcto y no afirma que esa versión existía históricamente.

## 6. Detección, idempotencia y retries

1. Lee la última versión exitosa para `Dataset1` + asset/tabla + URL.
2. Ejecuta HEAD con `If-None-Match` cuando existe un ETag anterior.
3. Un `304` válido o ETag idéntico registra `SKIPPED_UNCHANGED` sin GET.
4. Primera carga, ETag distinto, HEAD no soportado o ausencia de ETag ejecutan GET condicional.
5. La respuesta GET aporta la versión efectiva; un cambio entre HEAD y GET queda auditado. Sin ETag fiable siempre se descarga y se compara SHA-256: tamaño o fecha iguales no prueban identidad.
6. Una versión nueva se archiva en `original/<asset>/<sha256>/`, se prepara y se publica. RAW conserva duplicados originales del CSV; la idempotencia evita publicar dos veces el mismo snapshot.

HEAD y GET comparten un máximo de tres solicitudes HTTP por asset. Solo se reintentan timeout/conexión, `429` y `5xx`, con espera exponencial desde 10 s, tope de 120 s y respeto de `Retry-After`. `4xx`, URL/gzip/CSV inválidos y cambios incompatibles fallan sin retry. Kestra no añade un retry genérico, evitando multiplicar intentos.

La conexión/publicación Snowflake reintenta como máximo tres veces solo ante `OperationalError` transitorio, reutilizando el archivo ya capturado. Autenticación, MFA, objetos/permisos y errores SQL no se reintentan. Un `002043` se informa con la operación y sentencia/objeto afectados.

RAW y el checkpoint `SUCCESS` se confirman en la misma transacción. Un fallo no adelanta el ETag de referencia; al recuperar, se borra solo un residuo de esa URL/checksum antes de insertar, sin duplicar la versión.

## 7. Auditoría en Snowflake

Consultar las últimas 25 ejecuciones desde PowerShell, usando las credenciales ya
inyectadas en Kestra y sin imprimirlas:

```powershell
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml audit --limit 25
```

El límite aceptado es de 1 a 1000. Para análisis SQL directo:

```sql
SELECT BATCH_ID, MODE, LOGICAL_DATE, TRIGGER_SOURCE, STATUS,
       STARTED_AT, FINISHED_AT, SUMMARY, ERROR
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_BATCHES
ORDER BY STARTED_AT DESC;

SELECT DATASET, ASSET, SOURCE_URL, STATUS,
       LOGICAL_DATE, SOURCE_CHECKED_AT, CAPTURED_AT,
       HEAD_STATUS, GET_STATUS,
       REMOTE_ETAG, REMOTE_LAST_MODIFIED, REMOTE_CONTENT_LENGTH,
       DOWNLOAD_ETAG, DOWNLOAD_LAST_MODIFIED, DOWNLOAD_CONTENT_LENGTH,
       SOURCE_VERSION, SOURCE_FILE_SHA256, HTTP_ATTEMPTS,
       REFERENCE_RUN_ID, SKIP_REASON, ROWS_READ, ROWS_LOADED, ERROR
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_FILES
ORDER BY STARTED_AT DESC;

SELECT COUNT(*) FROM FOOTBALL.BRONZE.COMPETITIONS_RAW;
SELECT COUNT(*) FROM FOOTBALL.BRONZE.COMPETITIONS_LATEST;
LIST @FOOTBALL.BRONZE.TRANSFERMARKT_BRONZE_STAGE/original/competitions/;
```

Los mensajes y controles no contienen contraseñas. `.env`, claves y artefactos locales están ignorados por Git.

## 8. Pruebas y diagnóstico

```powershell
docker build --target test -t transfermarkt-bronze-tests:local .
docker run --rm transfermarkt-bronze-tests:local
docker compose config --quiet
```

Las pruebas simulan primera carga, novedad, ETag igual/diferente, `304`, falta de validadores, fallo sin checkpoint, recuperación sin duplicados, Schedule/manual/backfill y redacción de secretos.

Diagnóstico:

```powershell
docker compose ps
docker compose logs --tail 200 kestra
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml inspect-source --asset competitions
```

Un error HTTP nunca se interpreta como “sin cambios”. La fuente estaba declarada pausada por el proveedor al verificarse este repositorio; si continúa igual, el pipeline registra skips reales y no inventa novedades.

Referencias: [Schedule y `recoverMissedSchedules`](https://kestra.io/docs/workflow-components/triggers/schedule-trigger), [backfill finito](https://kestra.io/docs/concepts/backfill), [retries de Kestra](https://kestra.io/docs/workflow-components/retries), [conector Python Snowflake](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect).
