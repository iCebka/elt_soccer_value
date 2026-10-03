# Progreso de implementación Silver

Actualizado: 3 de octubre de 2026 (`America/Guayaquil`).

## Etapa 1 de 8 — completa

Infraestructura reproducible de dbt y contrato de entrada implementados y verificados. No se implementaron transformaciones Silver, Gold ni componentes predictivos. Bronze, sus tablas/vistas, la ingesta y los flows existentes no fueron modificados.

### Archivos

- `docker/dbt/Dockerfile`: Python 3.12.12 fijado por digest, `git` para dbt y artefactos en `/tmp`.
- `docker/dbt/requirements.in` y `requirements.lock`: `dbt-core==1.12.5`, `dbt-snowflake==1.12.1` y 79 dependencias transitivas fijadas con hashes para Linux/Python 3.12.
- `dbt/dbt_project.yml`, `dbt/profiles.yml`: targets `dev`/`prod`, password auth, mismo rol/warehouse/database y materializaciones por carpeta.
- `dbt/macros/generate_schema_name.sql`: prefijo aislado en `dev` y schemas exactos en `prod`.
- `dbt/macros/report_effective_schemas.sql`: comprobación local, sin consulta a Snowflake.
- `dbt/models/staging/_sources.yml`: 12 vistas `*_LATEST` y dos objetos de auditoría.
- `dbt/models/{staging,silver}/_*_contract.sql`: sentinelas deshabilitados; validan configuración y no crean relaciones.
- `dbt/analyses/profile_required_keys.sql`: perfil de solo lectura para las claves exigidas.
- `docker-compose.yml`: servicio one-shot `dbt` bajo el profile `silver`, con las variables Snowflake/Silver explícitas y sin claves privadas.
- `.env.example`, `.gitignore`, `.dockerignore`: variables Silver y exclusión de secretos/artefactos.
- `docs/silver_transfermarkt.md`: inventario, tipos físicos, encabezados, carga, keys, schemas y contrato de salida.
- `tests/test_dbt_scaffold.py`: cobertura estática del scaffold, sources, auth y lock.
- `README.md`: uso y alcance de la infraestructura dbt.

### Decisiones

- La instalación comprobada permanece en Kestra `2.0.4`, `plugin-script-python:1.13.0` y Process runner. Esta etapa usa `docker compose run --rm dbt`; no añade un tipo de task/plugin de contenedores no instalado. La coordinación Kestra se implementará en una etapa posterior.
- dbt reutiliza `SNOWFLAKE_ACCOUNT`, `USER`, `PASSWORD`, `ROLE`, `WAREHOUSE`, `DATABASE` y `SNOWFLAKE_BRONZE_SCHEMA`. Solo se añadieron `DBT_TARGET`, `DBT_DEV_SCHEMA`, `SNOWFLAKE_STAGING_SCHEMA` y `SNOWFLAKE_SILVER_SCHEMA`.
- `dev` es el target predeterminado: `DBT_DEV_STAGING` (views) y `DBT_DEV_SILVER` (tables). `prod` resuelve exactamente a `STAGING` y `SILVER`.
- Se reutilizó la autenticación existente: `SNOWFLAKE_AUTH_METHOD=password` y `authenticator: snowflake`. No se modificó `.env`; no hay lectura/montaje de `.p8`, private key ni passphrase.
- Las fuentes son snapshots completos. Silver leerá `*_LATEST`; RAW conserva versiones distintas y los skips no duplican filas.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| `docker compose config --quiet` | OK, código 0; no se imprimió configuración expandida. |
| consulta de metadata PyPI | OK; las versiones elegidas declaran Python `>=3.10` y compatibilidad core `>=1.10,<2.0`. |
| `uv pip compile ... --python-platform x86_64-unknown-linux-gnu --generate-hashes` | OK, 79 paquetes resueltos. |
| suite con Python 3.12 administrado por uv | Bloqueada al importar `_ssl` por Control de aplicaciones de Windows; no se declaró como pase. |
| suite con Python 3.14 local, mismas dependencias de proyecto | OK, `32 passed`. |
| `dbt parse` local con core `1.12.5` / adapter `1.12.1` | OK. |
| `report_effective_schemas`, targets `dev` y `prod` | OK: `DBT_DEV_STAGING`/`DBT_DEV_SILVER`; `STAGING`/`SILVER`. |
| `docker compose --profile silver build dbt` | OK; imagen `transfermarkt-dbt:1.12.5-snowflake-1.12.1`. |
| `docker compose --profile silver run --rm dbt parse --no-version-check` | OK dentro de la imagen. |
| primer `dbt debug` | Falló solo por ausencia de `git`; se corrigió la imagen y se repitió. |
| `dbt debug --target dev` final | OK, conexión real validada con salida redactada. |
| `dbt show --inline "select 1 ..."` | OK, `SELECT 1` real. |
| auditoría Bronze de solo lectura | OK; 12 assets con `SUCCESS` completo. Conteos registrados en `silver_transfermarkt.md`. |
| perfil de `appearance_id` | 1,894,350 filas; 0 claves vacías; 0 duplicados excedentes. |
| perfil de (`player_id`, `TRY_TO_DATE(date)`) | 656,301 filas; 0 claves inválidas; 0 duplicados excedentes. |
| `git diff --check` y búsqueda de secretos rastreados | OK; ningún valor sensible del `.env` aparece en archivos rastreados/nuevos. |

La salida de las comprobaciones de conexión se capturó y sustituyó con `[REDACTED]` usando todos los valores no vacíos de `.env`. No se ejecutó `dbt run`, DDL Silver ni una ingesta adicional. Como autenticación no cambió, no se repitió una carga aislada ni se alteraron controles Bronze productivos.

### Dependencias transferidas a la etapa 2 (histórico)

- implementar vistas staging tipadas, normalización de vacíos y observabilidad de conversiones;
- convertir las claves ya perfiladas y las restantes en tests dbt permanentes;
- verificar los privilegios de creación sobre los schemas de salida cuando se autorice el primer `dbt run` (la conexión y `SELECT 1` sí están validados);
- diseñar el flow Kestra que coordine el contenedor dbt con un mecanismo compatible y explícitamente instalado;
- conservar Gold y el modelo predictivo fuera de Silver.

Las dependencias de fuentes, limpieza, selección de versiones, pruebas y
primer build aislado se resolvieron en la etapa 2 descrita a continuación. La
orquestación Kestra y los cuatro productos finales permanecen para sus etapas
correspondientes.

## Etapa 2 de 8 — completa

Se implementaron sources RAW, staging tipado, selección reproducible de
versiones, clasificación/deduplicación, entidades base aceptadas, quarantine,
reconciliación, aliases y pruebas para los 12 assets Transfermarkt. Se verificó
en `DBT_TEST_STAGING` / `DBT_TEST_SILVER`; Bronze y los schemas Silver
productivos no fueron alterados. No se implementaron los cuatro modelos finales,
Gold ni el modelo predictivo.

### Archivos

- `dbt/models/staging/transfermarkt/_sources.yml`: 12 tablas RAW y dos objetos
  de auditoría; staging no depende de `*_LATEST`.
- `dbt/models/staging/transfermarkt/stg_tm__*.sql` y
  `_stg_tm__models.yml`: 12 vistas tipadas, documentadas y con procedencia.
- `dbt/models/silver/control/base_tm__source_manifest.sql` y `_control.yml`:
  último `SUCCESS` por asset o manifiesto fijo completo.
- `dbt/models/silver/intermediate/int_tm__*_classified.sql`: 12
  clasificadores `ephemeral` con reglas de aceptación y conflictos.
- `dbt/models/silver/base/transfermarkt/base_tm__*.sql` y
  `_base_tm__models.yml`: 12 tablas de registros aceptados con pruebas de grain,
  tipos y relaciones.
- `dbt/models/silver/quality/base_tm__{quarantine,reconciliation}.sql`:
  rechazos explicables y balance por fuente.
- `dbt/macros/transfermarkt_cleaning.sql` y
  `transfermarkt_classification.sql`: conversiones tolerantes, importes EUR,
  fingerprint, duplicados y disposición.
- `dbt/macros/export_transfermarkt_manifest.sql`: exporta el mapa fijable sin
  secretos; `dbt/macros/tests/*.sql` aporta tres pruebas genéricas locales.
- `dbt/seeds/transfermarkt/`: aliases observados de posiciones, eventos y
  alineaciones, con documentación/pruebas.
- `dbt/tests/`: completitud del manifiesto y fixtures aislados de versiones,
  duplicados, nulos, conflictos e importes inválidos.
- `dbt/dbt_project.yml`, `dbt/profiles.yml`, `docker-compose.yml` y
  `.env.example`: tags de etapa 2 y target aislado `test` con
  `DBT_TEST_SCHEMA`.
- `tests/test_dbt_stage2.py` y `tests/test_dbt_scaffold.py`: contrato estático
  del grafo, sources, manifiesto, tags y sentinel de minuto desconocido.
- `docs/silver_transfermarkt.md` y `README.md`: contrato operativo, reglas,
  resultados y ejecución manual.

### Decisiones

- Cada build materializa una fila de manifiesto por asset y une RAW mediante
  `ingestion_run_id` + SHA-256. Esto permite combinar las últimas versiones
  válidas de distintas fuentes sin mezclar filas entre snapshots.
- Un mapa completo puede fijarse con la variable dbt
  `transfermarkt_source_manifest`. La ejecución automática y la manual usan el
  mismo modelo; se documentó `dbt build` para que la prueba de completitud
  detenga un mapa que no resuelva 12 assets.
- Vacíos y desconocidos quedan `NULL`; cada staging retiene `RAW_RECORD`. El
  sentinel real `game_events.minute=-1` (17.575 filas) se normaliza a `NULL`, no
  a rechazo. Otros negativos siguen siendo inválidos.
- Los importes numéricos fuente están en EUR y usan `NUMBER(38,2)`. Ausencia no
  se convierte en cero. `clubs.net_transfer_record` tiene texto con `k/m/bn` y
  admite saldo negativo; los demás importes negativos se rechazan.
- Duplicados idénticos se colapsan por fingerprint y fila fuente. Ante la misma
  clave con contenidos distintos se envían todos los representantes a
  quarantine; no se elige un ganador arbitrario. Transfers no recibe una clave
  sintética porque la fuente no expone `transfer_id`.
- `player_valuations` conserva el historial completo y protege la clave
  (`player_id`, `valuation_date`). `appearance_id` y las demás claves observadas
  quedaron protegidas por pruebas permanentes.
- Las relaciones con dimensiones actuales son `warn`: los snapshots de clubes,
  jugadores y competiciones no cubren todo el histórico de partidos. Claves,
  grains, manifiesto y reconciliación son errores críticos.
- La ingesta RAW actual es append-only; no necesita bloqueo. Quedó documentado
  que un futuro reemplazo físico deberá coordinarse en Kestra.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| perfiles de importes y categorías mediante `dbt show` | OK; unidades EUR, rangos, vacíos y valores observados documentados. |
| `dbt compile --target test --no-partial-parse --warn-error` | OK; 39 modelos, 69 data tests, 3 seeds y 14 sources. |
| suite local con Python 3.14 y dependencias fijadas | OK, `39 passed`; el launcher `pytest` fue bloqueado por Control de aplicaciones y se repitió correctamente como `python -m pytest`. |
| fixtures SQL de manifiesto y deduplicación/quarantine | PASS dentro de los builds reales. |
| primer `dbt build --target test --select +tag:silver_stage_2` | 94 PASS, 5 WARN, 0 ERROR, 99 nodos. El perfil posterior detectó que 17.575 minutos `-1` eran desconocidos; se corrigió y se reconstruyó. |
| `run-operation export_transfermarkt_manifest` | OK; exportó 12 pares run/checksum sin credenciales. |
| intentos de fijación previos al comando final | `DBT_VARS` no fue reconocido (`is_fixed_manifest=false`) y una forma JSON perdió quoting en PowerShell; no se contaron como validación. Se corrigió a `--vars` con YAML flow. |
| `dbt build ... --vars '{transfermarkt_source_manifest: ...}'` final | OK; 12/12 filas con `is_fixed_manifest=true`; 94 PASS, 5 WARN, 0 ERROR. |
| consulta de reconciliación final | 7.497.433 entrada, 7.497.433 aceptadas, 0 duplicados, 0 rechazos y 0 filas reales en quarantine. |
| repetición y `HASH_AGG(record_fingerprint)` | OK; los 12 conteos y las 12 huellas de negocio fueron idénticos antes/después, excluyendo timestamps Silver. |
| relaciones con cobertura parcial | WARN esperados: home clubs 12.818, away clubs 11.242, competitions 1.214, appearance players 2 y current clubs 2.986. |
| `docker compose config --quiet` | OK, código 0 y sin configuración expandida. |
| `docker compose --profile silver build dbt` | OK; imagen Stage 2 reconstruida con el lock existente. |
| `dbt parse --target test --no-partial-parse --warn-error` en la imagen | OK. |

Un intento local de la suite sin todas las dependencias falló durante
colección por `requests` ausente; no se contó como prueba. Se repitió con las
versiones de `requirements*.txt` y pasó. La conexión Snowflake y el permiso de
crear vistas/tablas en el target de prueba quedaron demostrados por los builds;
no se extrapola ese resultado a `prod`.

### Estado de datos verificado

- Los 12 grains declarados pasan; en particular, `appearance_id` y
  (`player_id`, `valuation_date`) siguen siendo únicos y no nulos.
- La ejecución fija conserva archivo, checksum, versión, captura, carga y run de
  origen por fila, además de `silver_processed_at`.
- El snapshot actual no contiene duplicados ni registros inválidos después de
  interpretar correctamente el sentinel `-1`. Los fixtures mantienen cobertura
  para que esos caminos no dependan de que aparezcan en producción.
- No falta ninguna de las nueve fuentes requeridas ni de las tres fuentes
  adicionales ya ingeridas.

### Dependencias para etapas posteriores

- construir los cuatro productos Silver finales sobre `ref('base_tm__*')`;
- decidir en sus etapas correspondientes cómo resolver la cobertura histórica
  indicada por las cinco advertencias, sin falsificar dimensiones;
- incorporar en Kestra la ejecución dbt y el traspaso del manifiesto con un
  mecanismo compatible con Kestra `2.0.4` y los plugins instalados;
- validar `prod` solo cuando se autorice; esta etapa usó exclusivamente el
  target aislado;
- conservar Gold y el modelo predictivo fuera de las siguientes etapas Silver.

## Etapa 3 de 8 — completa

Se implementó `silver_players_current` como tabla reproducible de una fila por
`player_id`, construida exclusivamente con `ref()` y `LEFT JOIN` sobre las
entidades limpias. El modelo se verificó en `DBT_TEST_SILVER`; no se alteraron
Bronze, la ingesta, Kestra, `.env` ni los schemas productivos. Gold y el modelo
predictivo permanecen fuera de alcance.

### Archivos

- `dbt/models/silver/final/silver_players_current.sql`: perfil actual,
  enriquecimientos opcionales, estados de resolución y procedencia separada.
- `dbt/models/silver/final/_silver_players_current.yml`: documentación,
  unicidad, relaciones, valores aceptados y contexto temporal.
- `dbt/models/silver/intermediate/int_tm__country_name_resolution.sql`: lookup
  determinista por nombre normalizado o alias explícito.
- `dbt/macros/transfermarkt_cleaning.sql`: macro
  `tm_country_name_key` para trim, espacios y mayúsculas.
- `dbt/seeds/transfermarkt/tm_country_name_aliases.csv` y `_seeds.yml`: aliases
  versionados `Turkey -> Türkiye` y `Macedonia -> North Macedonia`, con
  justificación.
- `dbt/tests/assert_country_{normalized_names_unique,aliases_resolve_once}.sql`:
  evita claves/aliases ambiguos.
- `dbt/tests/assert_silver_players_{preserves_players,resolution_consistent}.sql`:
  conservación, cardinalidad y consistencia de estados/IDs.
- `dbt/tests/assert_player_citizenship_is_scalar.sql`: detecta una futura
  necesidad de puente de ciudadanías.
- `dbt/tests/fixture_silver_players_current.sql`: casos aislados de selección
  ausente, roles distintos, país ausente y ambigüedad.
- `tests/test_dbt_stage3.py`: contrato estático de refs, joins, roles, aliases,
  contexto, lineage y tags.
- `docs/silver_players_current.md`, `README.md` y este archivo: contrato,
  cobertura observada y handoff al modelo de valoraciones.
- `dbt/dbt_project.yml`: tag `silver_stage_3` para modelo, lookup, seed y tests.

### Decisiones

- Se verificó primero la unicidad de `player_id`, `national_team_id`,
  `country_id`, `club_id` y del nombre de país normalizado: cero duplicados y
  máximo un candidato por clave.
- La selección se une solo con
  `players.current_national_team_id = national_teams.national_team_id`. La
  columna existe en la versión ingerida; no se infiere desde nacimiento,
  ciudadanía ni nombres.
- El país de selección se une solo con
  `national_teams.country_id = countries.country_id` y permanece separado de
  nacimiento y ciudadanía.
- Nacimiento y ciudadanía conservan el texto fuente y resuelven por trim,
  colapso de espacios y mayúsculas. No se usa matching difuso. Un nombre o
  alias ambiguo queda sin ID con estado `ambiguous`.
- Solo se aliasaron dos variantes presentes y justificables contra el catálogo.
  Países históricos, territorios ausentes y `United Kingdom` quedan
  `not_found`; no se fuerzan a una entidad actual.
- `country_of_citizenship` es escalar en el snapshot. No se observaron
  separadores múltiples; las comas pertenecen a `Korea, South/North`. No se
  creó `silver_player_citizenships`. Un test crítico detendrá futuras entradas
  con `/`, `;` o `|` hasta implementar el puente.
- Los IDs fuente de club/selección se conservan aunque no resuelvan; los IDs
  `resolved_*` quedan `NULL`. Las relaciones fuente incompletas son `warn` y
  las relaciones resueltas deben pasar.
- Club, selección, valor, contrato, acumulados internacionales y ranking FIFA
  están etiquetados como contexto del snapshot actual, no como estado as-of de
  fechas históricas.
- El modelo expone por separado versión, checksum, captura y run Bronze de
  players, national teams, countries y clubs, además de
  `silver_processed_at`.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| perfiles `dbt show` de claves y nombres | 0 duplicados en cuatro claves y 0 nombres normalizados ambiguos; `current_national_team_id` confirmado. |
| perfil de uniones por ID | 50.149 jugadores conservados; 46.889 sin selección, 106 selecciones no encontradas, 2.986 clubes no encontrados. |
| perfil previo de nombres | nacimiento: 38.994 exactos, 5.885 ausentes, 5.270 inicialmente no encontrados; ciudadanía: 45.980 exactos, 269 ausentes, 3.900 inicialmente no encontrados. |
| perfil de ciudadanías múltiples | 0 separadores múltiples; solo `Korea, South` (619) y `Korea, North` (2) contienen coma. |
| `dbt compile --target test --no-partial-parse --warn-error` | OK; 41 modelos, 100 data tests, 4 seeds, 14 sources y 583 macros. |
| suite local con dependencias fijadas | OK, `45 passed`. |
| `dbt build --target test --select +tag:silver_stage_3 --vars ...` | OK; manifiesto fijo, 69 nodos: 63 PASS, 6 WARN, 0 ERROR. |
| build repetido de `silver_players_current` y sus tests | 50.149 filas; 22 PASS, 2 WARN, 0 ERROR sobre 24 nodos. |
| cardinalidad/huella antes y después | 50.149 filas y 50.149 IDs; hash de negocio idéntico `-2143894216464144838`. |
| `docker compose config --quiet` | OK, código 0 y sin configuración expandida. |
| `docker compose --profile silver build dbt` | OK; imagen reconstruida con Stage 3. |
| `dbt parse --target test --no-partial-parse --warn-error` en la imagen | OK. |

Las seis advertencias del build con dependencias fueron coberturas conocidas:
clubs históricos en games (11.242 away y 12.818 home), dos players históricos
en appearances, 2.986 current clubs sin dimensión (base y modelo final) y 106
current national teams sin dimensión. Las relaciones de todos los IDs resueltos
pasaron.

### Cobertura del modelo

| Rol | Coincidencia directa | Alias | Ausente | No encontrado | Ambiguo |
|---|---:|---:|---:|---:|---:|
| nacimiento | 38.994 | 1.242 | 5.885 | 4.028 | 0 |
| ciudadanía | 45.980 | 1.392 | 269 | 2.508 | 0 |

- 3.372 jugadores tienen nacimiento y ciudadanía resueltos a países
  distintos; ambos roles se conservan.
- 3.154 jugadores resuelven selección y país de selección; 46.889 no tienen
  ID de selección y 106 IDs no están en el snapshot dimensional.
- `country_id` se documenta como identificador Transfermarkt, no ISO.

### Contrato para la etapa de valoraciones

`silver_player_valuations_enriched` podrá hacer `LEFT JOIN` por `player_id`
contra `silver_players_current` sin multiplicar su grain
(`player_id`, `valuation_date`). Debe conservar `valuation_date` como fecha del
hecho y tratar todos los atributos de este modelo como contexto del snapshot
actual, no como estado histórico del jugador en esa fecha.

### Dependencias para etapas posteriores

- construir `silver_games_enriched`, `silver_player_match` y
  `silver_player_valuations_enriched` en sus etapas asignadas;
- si una captura futura trae ciudadanías múltiples, implementar
  `silver_player_citizenships` antes de aceptar esa versión;
- ampliar el catálogo solo con fuentes/aliases verificables; no resolver
  automáticamente los 4.028 nacimientos ni 2.508 ciudadanías hoy no
  encontradas;
- mantener la coordinación Kestra, Gold y el modelo predictivo para sus etapas
  posteriores.

## Etapa 4 de 8 — completa

Se implementó `silver_games_enriched` como tabla reproducible de una fila por
`game_id`, construida con `ref()` y `LEFT JOIN` sobre games, competitions y
clubs limpios. Se materializó y verificó en `DBT_TEST_SILVER`; no se modificaron
Bronze, la ingesta, Kestra, `.env` ni schemas productivos. Gold y el modelo
predictivo permanecen fuera de alcance.

### Archivos

- `dbt/models/silver/final/silver_games_enriched.sql`: partido enriquecido,
  roles local/visitante, semántica temporal, estados de resolución y
  procedencia de las tres fuentes.
- `dbt/models/silver/final/_silver_games_enriched.yml`: documentación,
  unicidad, tipos, relaciones, nulos y valores aceptados.
- `dbt/tests/assert_silver_games_inputs_unique.sql`: protege la unicidad de
  `game_id`, `competition_id` y `club_id` antes de las uniones.
- `dbt/tests/assert_silver_games_preserves_games.sql`: reconcilia conjunto de
  claves, filas y ausencia de multiplicación.
- `dbt/tests/assert_silver_games_resolution_consistent.sql`: valida estados e
  IDs resueltos por los tres roles.
- `dbt/tests/assert_silver_games_valid_game_fields.sql`: fecha, participantes,
  goles, asistencia y posiciones según las reglas observadas.
- `dbt/tests/fixture_silver_games_enriched.sql`: fixture aislado con prefijos,
  nombre histórico distinto, catálogos incompletos y partido de selección.
- `tests/test_dbt_stage4.py`: contrato estático de refs, joins, prefijos,
  temporalidad, lineage, tags y handoff.
- `docs/silver_games_enriched.md`, `README.md` y este archivo: contrato,
  cobertura real y campos que consumirá `silver_player_match`.

### Decisiones

- Se comprobaron antes de unir 88.958 `game_id` únicos, 65
  `competition_id` únicos y 796 `club_id` únicos. Las verificaciones también
  quedan como tests permanentes.
- Las únicas uniones dimensionales son
  `games.competition_id = competitions.competition_id`,
  `games.home_club_id = home_club.club_id` y
  `games.away_club_id = away_club.club_id`; todas son `LEFT JOIN`.
- Se conservaron 88.958 partidos. Resuelven 87.744 competiciones, 76.140
  participantes locales y 77.716 visitantes. Permanecen 1.214 competiciones,
  12.818 locales y 11.242 visitantes sin catálogo.
- Los 742 partidos `national_team_competition` no resuelven participantes
  contra `clubs`; se mantienen con IDs/nombres fuente. No se fuerzan contra
  `national_teams` por nombre ni se eliminan.
- Nombres, tipo de competición, posiciones, managers y formaciones de `games`
  llevan semántica `_at_game`. Los pocos campos descriptivos de `clubs` llevan
  `_current_snapshot`. No se exponen coach, plantilla ni valor actual como si
  fueran históricos.
- `competition_type_at_game` y `competition_type_snapshot` quedan separados.
  Hay cero diferencias en la captura cuando el catálogo resuelve, pero no se
  coalescen para ocultar una futura diferencia.
- `competition_country_id` es un ID Transfermarkt. El nombre de país es
  opcional: falta en 11.236 partidos con competición resuelta y permanece
  `NULL`; no se infiere.
- Los nombres históricos coinciden con el catálogo en las filas resueltas. En
  `games` faltan 86 nombres locales y 36 visitantes; no se sobrescriben con el
  snapshot actual.
- El intervalo observado es 2006-06-09 a 2026-07-06. No hay fechas o goles
  ausentes, goles negativos, participantes iguales, asistencias negativas ni
  posiciones menores que uno. No se impuso una relación artificial entre
  `season` y el año natural.
- El modelo conserva versión, checksum, captura y run por separado para
  `games`, `competitions` y `clubs`. `source_version` y `source_captured_at`
  están ausentes en la auditoría actual y se mantienen `NULL`; no se inventan.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| perfiles `dbt show` de cardinalidad, cobertura, fechas, resultados y nombres | 88.958 partidos únicos; catálogos únicos; rangos y coberturas documentados arriba. |
| `docker compose config --quiet` | OK, código 0; no se imprimió configuración expandida. |
| `dbt parse --target test --no-partial-parse --warn-error` | OK; 42 modelos, 128 data tests, 4 seeds y 14 sources. |
| suite local completa con dependencias declaradas | OK, `50 passed`. El `python.exe` del sistema fue bloqueado y la prueba válida se ejecutó con el intérprete gestionado por `uv`. |
| `run-operation export_transfermarkt_manifest` | OK; exportó los 12 pares run/checksum sin secretos. El primer intento no inició por timeout de aprobación y no se contó. |
| primer `dbt build --select +tag:silver_stage_4 --vars ...` | El modelo creó 88.958 filas, pero un test propio refería los nombres previos de las posiciones y produjo 1 error de compilación SQL. Se corrigió; este intento no se cuenta como validación final. |
| build fijado final de Stage 4 | 58 nodos: 50 PASS, 8 WARN esperados, 0 ERROR. El fixture, conservación, unicidad, reglas y relaciones resueltas pasaron. |
| repetición `dbt run --select +silver_games_enriched --vars ...` | OK, 8/8 modelos; 88.958 filas y 88.958 IDs antes/después. |
| huella de negocio antes/después | Idéntica: `-9170258405739628664`, excluyendo solo `silver_processed_at`. |
| `git diff --check` | Sin errores de whitespace. |

Las ocho advertencias del build son cobertura, no fallos de claves resueltas:
las tres relaciones base de games y sus tres equivalentes finales (12.818 home,
11.242 away y 1.214 competiciones), más dos advertencias ya existentes de
2.986 clubes actuales de players activadas al seleccionar la dependencia
`clubs`. Todas las relaciones de `resolved_*_id` pasaron.

La conexión Snowflake y los permisos de lectura/creación en el target aislado
quedaron demostrados por perfiles, build y repetición. No se extrapola ese
resultado a `prod`.

### Contrato para la etapa 5

`silver_player_match` podrá unir
`appearances.game_id = silver_games_enriched.game_id`. La unicidad verificada
impide multiplicar `appearance_id`. Puede consumir `game_date`, `season`,
`game_round`, competición/país, IDs/nombres local y visitante, marcador,
campos `_at_game` y estados de resolución. Los campos `_current_snapshot`
siguen siendo contexto del catálogo actual y no historia as-of.

### Dependencias pendientes

- construir y validar `silver_player_match` en su etapa, conservando una fila
  por la clave real `appearance_id`;
- decidir solo en una etapa futura autorizada si se necesita un catálogo
  histórico adicional para participantes/competiciones hoy no resueltos; esta
  etapa no inventa esa cobertura;
- poblar versión/captura en Bronze únicamente si la fuente/auditoría real las
  proporciona; los `NULL` actuales son contractuales;
- mantener la coordinación dbt en Kestra, Gold y el modelo predictivo para sus
  etapas posteriores;
- validar `prod` solo cuando se autorice; Stage 4 usó exclusivamente `test`.

## Etapa 5 de 8 — completa

Se implementó `silver_player_match` como tabla reproducible de una fila por
`appearance_id`. Parte de appearances limpio y añade el partido y la
perspectiva del equipo mediante `LEFT JOIN`; no modifica Bronze, ingesta,
Kestra, `.env` ni schemas productivos. Se materializó y verificó únicamente en
`DBT_TEST_SILVER`. Gold y el modelo predictivo permanecen fuera de alcance.

### Archivos

- `dbt/models/silver/final/silver_player_match.sql`: contexto de partido,
  equipo/rival, resultado, estadísticas, precedencia de fecha/competición,
  estados de unión y trazabilidad.
- `dbt/models/silver/final/_silver_player_match.yml`: documentación, grain,
  relaciones, tipos, estados y checksums requeridos.
- `dbt/tests/assert_silver_player_match_inputs_unique.sql`: verifica
  `appearance_id`, `game_id` y (`game_id`, `club_id`) antes de unir.
- `dbt/tests/assert_silver_player_match_preserves_appearances.sql`: reconcilia
  claves, filas y ausencia de multiplicación.
- `dbt/tests/assert_silver_player_match_context_consistent.sql`: valida roles
  home/away, equipo, rival, marcador, resultado e `is_win`.
- `dbt/tests/assert_silver_player_match_comparisons_consistent.sql`: protege
  precedencia y reporte de discrepancias de fecha/competición.
- `dbt/tests/assert_silver_player_match_valid_stats.sql`: rechaza estadísticas
  negativas sin imponer máximos ni transformar nulos.
- `dbt/tests/fixture_silver_player_match.sql`: dos equipos en un partido,
  jugador que cambió de club, discrepancias, minutos 0/135 y filas sin
  club_games o sin games.
- `tests/test_dbt_stage5.py`: contrato estático de refs, clave compuesta,
  roles, precedencia, estadísticas, trazabilidad y límite con Gold.
- `docs/silver_player_match.md`, `docs/silver_transfermarkt.md`, `README.md` y
  este archivo: contrato, resultados y handoff.

### Decisiones

- Se verificó la clave real: 1.894.350 filas y 1.894.350 `appearance_id`.
  `silver_player_match` conserva exactamente ese grain.
- `club_games` tiene 177.916 combinaciones únicas (`game_id`, `club_id`). Unir
  solo por `game_id` produciría 3.788.700 filas; la unión implementada usa
  simultáneamente `appearances.game_id = club_games.game_id` y
  `appearances.player_club_id = club_games.club_id` y conserva 1.894.350.
- `player_club_id` es el equipo histórico de la participación y
  `player_current_club_id` permanece separado. Difieren en 1.090.673 filas, de
  modo que el actual nunca reemplaza al histórico.
- Fecha y competición conservan valores de appearances y games. Los campos
  canónicos prefieren games y usan appearances solo como fallback. Estados
  explícitos registran coincidencia, discrepancia o ausencia; no se corrige ni
  elimina silenciosamente.
- La captura tiene cobertura completa para games y club_games, cero
  discrepancias de fecha/competición y cero inconsistencias de equipo,
  oponente, marcador o `is_win`. Los fixtures cubren los caminos incompletos y
  discrepantes que no aparecen actualmente.
- Hay 938.329 participaciones home y 956.021 away. Equipo/rival y sus nombres
  se derivan de los roles ya contratados por `silver_games_enriched`.
- Se conservan 3.027 participaciones de selecciones y 14.200 participaciones
  cuya competición no resuelve contra el catálogo. No se inventan relaciones.
- `minutes_played` se conserva sin cap: 11.041 valores superan 90, tres superan
  120 (máximo 148) y tres valen cero. Solo se rechazan estadísticas negativas;
  ausencias futuras seguirán `NULL`, no cero.
- El modelo conserva por separado versión, checksum, captura y run para
  appearances, club_games, games, competitions y clubs. `silver_processed_at`
  no sustituye la fecha del partido.
- No se unen valoraciones por `player_id`: el cruce temporal rendimiento/valor
  será una transformación as-of de Gold en una etapa posterior.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| perfiles `dbt show` de grains y joins | Appearances 1.894.350/1.894.350 únicos; club_games 177.916/177.916 claves compuestas; join solo por game 3.788.700 frente a 1.894.350 con la clave correcta. |
| perfiles de cobertura y discrepancias | 0 games ausentes, 0 club_games ausentes, 0 discrepancias de fecha/competición y 1.090.673 clubes actuales distintos al histórico. |
| perfil de estadísticas especiales | minutos 0..148; 3 ceros, 11.041 sobre 90 y 3 sobre 120; no negativos ni nulos observados. |
| `dbt parse --target test --no-partial-parse --warn-error` | OK. El proyecto contiene 43 modelos, 158 data tests, 4 seeds y 14 sources. |
| pruebas estáticas Stage 5 | Un primer intento tuvo 1 fallo por comparar texto a través de un salto de línea; se normalizó whitespace y la repetición válida dio `6 passed`. |
| `dbt build --target test --select +tag:silver_stage_5 --vars ...` | Manifiesto fijo reutilizado; 99 nodos: 89 PASS, 10 WARN esperados, 0 ERROR. `silver_player_match` creó 1.894.350 filas. |
| consulta agregada final del modelo | 1.894.350 filas/IDs, 1.894.350 games y club_games resueltos, 938.329 home, 956.021 away, 0 inconsistencias y 0 discrepancias. |

No se repitieron la ingesta, la suite amplia del repositorio, Compose ni pruebas
de autenticación: no cambiaron esos componentes y habían pasado en etapas
anteriores. Las verificaciones se limitaron al modelo, sus dependencias y sus
contratos nuevos, según el alcance solicitado.

Las diez advertencias corresponden a cobertura ya conocida: dos `player_id` de
appearances ausentes del snapshot players (en base y final), las seis relaciones
base/final de games contra catálogos incompletos, y las dos relaciones existentes
de 2.986 clubes actuales de players. Ninguna elimina participaciones; claves,
conteos, relaciones con games y estados de contexto pasaron.

### Dependencias pendientes

- implementar `silver_player_valuations_enriched` con grain validado
  (`player_id`, `valuation_date`) y contexto de jugador claramente etiquetado;
- no cruzar `silver_player_match` con valoraciones solo por `player_id`; definir
  el as-of temporal en Gold cuando esa etapa sea autorizada;
- conservar los caminos de fallback y cobertura incompleta aunque el snapshot
  actual resuelva todos los games/club_games;
- poblar `source_version`/`source_captured_at` únicamente si Bronze recibe esos
  valores reales; no inventarlos en Silver;
- mantener Kestra, Gold, modelo predictivo y validación `prod` para sus etapas
  correspondientes.

## Etapa 6 de 8 — completa

Se implementó `silver_player_valuations_enriched` como historial reproducible
de una fila por (`player_id`, `valuation_date`). El enriquecimiento usa un único
`LEFT JOIN` a `silver_players_current`; conserva valoraciones sin perfil y no
supone relaciones directas con selecciones o países. Se materializó y verificó
en `DBT_TEST_SILVER` sin modificar Bronze, ingesta, Kestra, `.env` ni schemas
productivos. Gold y el modelo predictivo permanecen fuera de alcance.

### Archivos

- `dbt/models/silver/final/silver_player_valuations_enriched.sql`: historial,
  valor EUR, edad, contexto snapshot, comparación de clubes, estados y lineage.
- `dbt/models/silver/final/_silver_player_valuations_enriched.yml`: grain,
  tipos, relaciones, valores aceptados y metadatos requeridos.
- `dbt/tests/assert_silver_player_valuations_inputs_unique.sql`: protege la
  clave candidata y la unicidad del perfil consultado.
- `dbt/tests/assert_silver_player_valuations_preserves_history.sql`: reconcilia
  filas, jugadores, claves, suma, mínimo, máximo y ceros.
- `dbt/tests/assert_silver_player_valuations_age_consistent.sql`: verifica años
  completos antes/después del cumpleaños y estados no calculables.
- `dbt/tests/assert_silver_player_valuations_context_consistent.sql`: valida
  join del perfil y comparación de club sin multiplicación.
- `dbt/tests/assert_silver_player_valuations_valid_amounts.sql`: conserva cero
  y rechaza únicamente importes nulos/negativos ya excluidos por base.
- `dbt/tests/fixture_silver_player_valuations_enriched.sql`: varias
  valoraciones, cumpleaños, perfil/selección/país ausentes y €0.
- `tests/test_dbt_stage6.py`: contrato estático de refs, historia, edad,
  temporalidad, lineage y límite con Gold.
- `docs/silver_player_valuations_enriched.md`: contrato detallado del modelo.
- `docs/silver_contract.md`: contrato consolidado de las cuatro tablas Silver.
- `docs/silver_transfermarkt.md`, `README.md` y este archivo: navegación,
  estado y handoff.

### Decisiones

- La clave candidata observada es válida: 656.301 filas y 656.301 pares
  (`player_id`, `valuation_date`) para 41.528 jugadores. Se conserva el historial
  completo; 40.465 jugadores tienen varias valoraciones y el máximo es 57.
- La única relación de enriquecimiento es
  `player_valuations.player_id = silver_players_current.player_id`. La cadena
  player → national_team → country ya está resuelta dentro del perfil y no se
  recrea ni se enlaza directamente desde valoraciones.
- `historical_market_value_eur` se distingue de
  `player_current_market_value_eur_snapshot`. Los valores observados van de €0
  a €200M, suman €1.502.990.157.999, incluyen un cero válido y no incluyen
  nulos/negativos.
- Los campos `current_club_*` de player_valuations se renombran
  `valuation_source_current_club_*`. No se afirman como club histórico as-of:
  18.821 jugadores presentan varios IDs, 36.489 varios nombres y 238.618 filas
  difieren del club del snapshot players. La comparación queda explícita.
- La edad usa fecha de nacimiento y resta un año antes del cumpleaños. El
  fixture prueba el día anterior (19) y el cumpleaños (20). En datos reales hay
  655.701 edades calculadas, 600 sin nacimiento, ninguna valoración anterior al
  nacimiento y rango 2..45. No se impone un filtro arbitrario de plausibilidad.
- Perfil, club, selección, ciudadanía, países, ranking FIFA, contrato, valor
  actual, máximo histórico y acumulados internacionales llevan semántica
  `snapshot`; no se presentan como información disponible en `valuation_date`.
- La ciudadanía sigue siendo escalar según el contrato verificado en Stage 3;
  no existe puente que expanda el grain. Un futuro puente deberá consumirse
  aparte o agregarse antes de relacionarlo con valoraciones.
- El modelo conserva versión, checksum, captura y run para player_valuations,
  players, national_teams, countries y clubs. Los metadatos ausentes permanecen
  `NULL`; no se fabrican desde fechas de negocio/procesamiento.
- Etiqueta, ventanas de rendimiento y cruces as-of con
  `silver_player_match` permanecen reservados para Gold.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| perfiles `dbt show` de grain/cobertura | 656.301 filas/claves, 41.528 jugadores, 656.301 perfiles resueltos y 0 ausentes. |
| perfiles de importes y fechas | 2000-01-20..2026-06-12; €0..€200M; suma €1.502.990.157.999; 1 cero, 0 nulos/negativos. |
| perfiles de edad y clubes | 600 sin nacimiento, 0 antes de nacer, edad 2..45; 417.683 clubes coinciden y 238.618 discrepan del snapshot. |
| `dbt parse --target test --no-partial-parse --warn-error` | OK. El proyecto contiene 44 modelos, 181 data tests, 4 seeds y 14 sources. |
| pruebas estáticas Stage 6 | OK, `6 passed`. |
| `dbt build --target test --select +tag:silver_stage_6 --vars ...` | Manifiesto fijo reutilizado; 106 nodos: 97 PASS, 9 WARN esperados, 0 ERROR. El modelo creó 656.301 filas. |
| consulta agregada final | 656.301 claves; 656.301 perfiles; 655.701 edades calculadas; 600 sin nacimiento; importes reconciliados. |

No se repitieron ingesta, Compose, autenticación ni la suite amplia: esos
componentes no cambiaron. Las nueve advertencias son coberturas heredadas de
dependencias seleccionadas (clubs en games/players, dos players históricos de
appearances y 106 selecciones actuales no presentes). La relación de las
valoraciones con perfiles tuvo cobertura completa y todas las pruebas nuevas
pasaron.

### Contrato completo y dependencias pendientes

Las cuatro tablas finales y sus grains quedan documentados en
`docs/silver_contract.md`:

- `silver_players_current`: una fila por `player_id`;
- `silver_games_enriched`: una fila por `game_id`;
- `silver_player_match`: una fila por `appearance_id`;
- `silver_player_valuations_enriched`: una fila por (`player_id`,
  `valuation_date`).

Para las etapas restantes queda pendiente:

- orquestar en Kestra el build Silver con manifiesto fijo y selección de tags,
  usando mecanismos compatibles con Kestra 2.0.4 y sin cambiar Bronze;
- definir solo en Gold el corte temporal, joins as-of, ventanas, etiqueta y
  features; nunca usar snapshots actuales como predictores históricos sin
  evidencia temporal;
- mantener el modelo predictivo fuera de alcance hasta su etapa;
- validar `prod` únicamente cuando se autorice; Stage 6 usó `test`.

## Etapa 7 de 8 — completa

Se implementó y registró el flow independiente
`football.transfermarkt.transfermarkt_silver`. Orquesta una copia inmutable del
proyecto dbt en el contenedor fijado, construye staging/bases/calidad y las
cuatro tablas finales con un manifiesto Bronze fijo, y avanza un checkpoint
separado solo tras validar todo el build. Bronze conserva su ingesta, detección
ETag/checksum, checkpoints y único Schedule mensual; Gold y el modelo
predictivo no se modificaron.

### Archivos

- `kestra/flows/transfermarkt_silver.yml`: preflight/plan, tarea Docker dbt,
  captura de artifacts, validación final, fallo explícito y ejecución manual.
- `kestra/flows/transfermarkt_ingest_bronze.yml`: único encadenamiento mediante
  Subflow con espera y transmisión de fallo, después del éxito Bronze.
- `src/ingestion/silver.py`: manifiesto, fingerprint, decisión, preflight,
  auditoría, checkpoint, conteos, evidencia quarantine y consulta de auditoría.
- `src/ingestion/snowflake.py`: factor común del conector password usado por
  Bronze y Silver, manteniendo `authenticator="snowflake"` y retry clasificado.
- `dbt/orchestration/run_dbt.py`: ejecución de `dbt build`, redacción,
  artifacts y hasta tres intentos solo para errores transitorios reconocidos.
- `dbt/dbt_project.yml`: tag unificado `transfermarkt_silver` para modelos,
  seeds y data tests.
- `docker-compose.yml` y `.env.example`: proyecto dbt read-only, socket Docker,
  imagen dbt y variables Silver dentro de Kestra y del contenedor hijo. Los
  aliases `ENV_*` son los que Kestra expone al namespace Pebble `envs`; los
  valores siguen viniendo del `.env` local.
- `scripts/register-flows.ps1` y `scripts/run-silver-flow.ps1`: registro en
  orden y ejecución manual normal/force/backfill.
- `Dockerfile` y `.dockerignore`: la imagen de tests incluye contratos dbt,
  flows, scripts, Compose y docs; runtime conserva las rutas existentes.
- `tests/test_silver_orchestration.py`: fixtures para fingerprint local,
  manifiesto/backfill, skip, force, recuperación, concurrencia, calidad,
  retries, tags, Compose y encadenamiento.
- `docs/silver_orchestration.md`, `README.md` y este archivo: operación,
  seguridad, auditoría, resultados y límites.

### Decisiones

- Se comprobó el entorno real: Kestra `2.0.4`, plugin Python `1.13.0`, Process
  runner y `plugin-docker:1.6.2`. Se eligió la tarea instalada
  `io.kestra.plugin.docker.cli.Run`; no se añadió un runner inexistente ni otro
  servicio de transformación.
- Existe un solo mecanismo Bronze → Silver: Subflow `wait: true` y
  `transmitFailed: true`. Silver no tiene trigger. El trigger Bronze conserva
  `0 6 1 * *`, `America/Guayaquil`, `recoverMissedSchedules: NONE` y
  `disabled: true`; no se habilitó una programación nueva.
- Bronze llama Silver después de cualquier comprobación completa exitosa,
  incluso con todos los archivos sin cambios. La decisión Silver nunca depende
  de `loaded` en el batch actual, por lo que un fallo pendiente se recupera en
  la siguiente revisión.
- `plan` selecciona o valida 12 versiones `SUCCESS` y comprueba las filas RAW
  por run/checksum. Bronze es append-only; no hay tablas reemplazadas que
  requieran un lock cruzado. Silver usa `concurrency: 1/QUEUE` y rechaza otro
  run reciente `RUNNING` del mismo target.
- El fingerprint usa ruta y bytes de SQL, YAML, macros, seeds, tests y
  configuración dbt: detecta cambios locales sin commit. La copia tar
  determinista evita que el proyecto cambie durante una ejecución.
- Se ejecuta por force, salida requerida ausente, falta de checkpoint, versión
  Bronze pendiente o transformación cambiada. Solo se omite si tablas,
  manifiesto y transformación ya están validados.
- El tag común ejecutó 4 seeds, 12 vistas, 19 tablas y 181 data tests; los 13
  modelos ephemeral forman parte del grafo pero no son nodos ejecutables.
- Los controles `TRANSFERMARKT_SILVER_RUNS`,
  `TRANSFERMARKT_SILVER_RUN_SOURCES` y
  `TRANSFERMARKT_SILVER_CHECKPOINT` viven en el schema Silver efectivo por
  target. Guardan modo, manifiesto, revisión, tiempos, resultado, conteos,
  artifacts y procedencia individual de cada fuente.
- El contenedor dbt devuelve cero al task Docker después de persistir evidencia;
  `finalize` interpreta el exit code real/run_results y el task Fail propaga un
  error crítico. Así los artifacts disponibles también sobreviven a fallos sin
  validar el run ni avanzar el checkpoint.
- No se promete atomicidad global. Un fallo dbt puede dejar objetos
  materializados; solo el checkpoint define qué ejecución queda habilitada
  para etapas posteriores.
- Backfill exige un manifiesto completo de versiones Bronze conservadas. No se
  usa la fecha lógica para fabricar historia ni para evaluar frescura.

### Comandos y resultados

| Comando/comprobación | Resultado |
|---|---|
| inspección de JAR/plugins de la imagen Kestra | Confirmados Kestra `2.0.4`, Python `1.13.0` y Docker `1.6.2`; `Run` expone image, entrypoint, env, input/output files y controles runtime. |
| `docker compose config --quiet` | OK después de montar proyecto/socket y propagar las variables necesarias. No imprimió la configuración expandida. |
| `docker compose --profile silver build dbt` | OK; imagen `transfermarkt-dbt:1.12.5-snowflake-1.12.1` reconstruida con el runner dbt. |
| `dbt parse --target test --no-version-check --no-partial-parse --warn-error` | OK con dbt `1.12.5` y adapter Snowflake `1.12.1`. |
| `dbt ls --select +tag:transfermarkt_silver` | Selección válida; el build real confirmó 44 modelos (13 ephemeral), 181 tests, 4 seeds, 14 sources y 583 macros. |
| primeras ejecuciones de pytest en la imagen | Stage 7 dio 6 PASS/3 FAIL y la suite 68 PASS/3 FAIL porque la etapa `test` histórica no copiaba flows/Compose/docs. Se completó únicamente ese contexto de test y se repitió. |
| pruebas aisladas Stage 7 / suite reproducible final | `11 passed` / `73 passed`; incluye éxito/skip/force/recuperación/concurrencia/backfill y demuestra tres intentos transitorios frente a uno para calidad. |
| recreación `postgres kestra` / `kestra` | OK sin borrar volúmenes; PostgreSQL se conservó y Kestra quedó healthy. |
| `scripts/register-flows.ps1` | API Kestra aceptó los tres flows. El primer POST Silver detectó dos incompatibilidades reales (`BOOLEAN` y `condition`); se corrigieron a `BOOL` y `runIf`. |
| ejecución `SAzw3lVZUpHDo9w42MeoS` | `FAILED` antes de dbt porque Kestra solo publica `ENV_*` en Pebble `envs`; error visible, handler ejecutado y sin checkpoint. Se añadieron aliases derivados sin poner secretos en código/logs. |
| recuperación `6VVj0tYPDb1SrfZQmlllu9` en `test` | `SUCCESS` sin nueva ingesta. dbt: `PASS=205 WARN=11 ERROR=0`, total 216, un intento. Checkpoint avanzado al final. |
| conteos auditados | players 50.149; games 88.958; player_match 1.894.350; valuations 656.301; reconciliation 12; quarantine 0. |
| artifacts auditados | `manifest.json`, `run_results.json`, plan, log/metadata de intento y compilados entre 454 artifacts dbt; `finalize` capturó además summary y evidencia quarantine. |
| ejecuciones normales `2SO3lSsaeIbK5vJGySFKIU` y `2y9ddooCL0xN4OUt576NUo` | `SUCCESS` con `run_dbt`/`finalize` `SKIPPED`; razón auditada `already_validated`. La segunda usó la imagen Kestra final. |
| comando `ingestion.silver audit --target test --limit 3` | Conexión real OK; mostró FAILED → SUCCESS → SKIPPED, un solo checkpoint exitoso, manifest/transformation fingerprints y 12 fuentes por run. |

Los 11 warnings son deliberados y corresponden a cobertura incompleta ya
contratada: games contra clubs (home/away) y competitions, appearances contra
players, players contra clubs/national teams, y las comprobaciones equivalentes
en los modelos finales. No hubo warning de grain, manifiesto, reconciliación o
preservación. No se ejecutó nuevamente la ingesta Bronze.

### Verificaciones pendientes / handoff a etapa 8

- `force`, exclusión concurrente y rechazo de backfill sin manifiesto se
  verificaron con fixtures/mocks, no mediante reconstrucciones Snowflake
  adicionales; la ruta normal, recuperación y skip sí se ejecutaron en `test`.
- El encadenamiento Bronze → Silver quedó registrado y validado estáticamente,
  pero no se lanzó Bronze solo para probarlo, pues habría repetido la ingesta.
- No se probó un backfill contra una versión alternativa porque no se solicitó
  ni demostró otra combinación histórica conservada. Las versiones actuales
  tienen checksum/run reales; `source_version` y captura permanecen `NULL`
  donde la auditoría Bronze legada no los contiene.
- No se ejecutó `prod` y el Schedule sigue desactivado. Validarlos/activarlo
  requiere autorización operativa y preflight productivo en la etapa
  correspondiente.
- Etapa 8 debe hacer la aceptación/handoff final de Silver. Gold, cruces as-of,
  features, etiqueta y modelo predictivo continúan explícitamente fuera de
  alcance.

## Etapa 8 de 8 — cerrada en target aislado

La aceptación integral de Silver quedó implementada y ejecutada en
`DBT_TEST_STAGING` / `DBT_TEST_SILVER`. Se verificaron desde el flow Kestra los
cuatro productos, todas sus dependencias y pruebas, la repetibilidad, el cambio
de código, `force`, omisión, cola de concurrencia y protección del checkpoint.
No se modificaron ni relanzaron Bronze, Gold o el modelo predictivo; tampoco se
escribió en los schemas `prod`.

### Correcciones y archivos de esta etapa

- `dbt/tests/assert_silver_delivery_contract.sql`: prueba crítica conjunta que
  reconcilia cada salida final contra su base y valida los cuatro grains.
- `src/ingestion/silver.py`: comando de solo lectura `verify`, huellas de
  negocio independientes del orden, métricas de entrega y salida de auditoría
  compacta (cantidad de artifacts en vez del arreglo completo).
- `tests/test_silver_orchestration.py`: prueba de exclusión de metadata de
  procesamiento/lineage y de que `finish(success=False)` no ejecuta ninguna
  mutación del checkpoint.
- `README.md`, `.env.example` y `docs/silver_transfermarkt.md`: procedimiento
  PowerShell comprobado, variables, schemas efectivos, grafo dbt, contratos,
  matching/rechazos, EUR, temporalidad y límite real del backfill.
- Este archivo: evidencia final, resultados y límites pendientes.

Se revisaron las etapas 1–7: permanecen 12 sources/staging/bases, cuatro seeds
de alias, clasificadores, quarantine/reconciliación, trazabilidad de versiones,
los cuatro finales y 182 data tests. La ciudadanía observada sigue siendo
escalar; por ello no hay puente que multiplique jugadores. El test existente
detiene un futuro valor múltiple hasta modelarlo explícitamente.

### Resultados integrales

| Comando/comprobación | Resultado real |
|---|---|
| `docker compose config --quiet` | Código 0; configuración Compose válida. |
| builds `dbt` y `kestra`, recreación solo de `kestra` | OK; volúmenes y PostgreSQL conservados. |
| suite reproducible `docker run --rm transfermarkt-bronze-tests:stage8 -m pytest -q` | OK, `75 passed`. |
| `dbt parse --target test --no-partial-parse --warn-error` | OK; 44 modelos, 182 tests, 4 seeds, 14 sources y 583 macros. |
| `dbt debug --target test` y `dbt show --inline "select 1"` | Conexión real OK y `connection_ok=1`; no se persistió su salida de identificadores en el repositorio. |
| registro de flows | OK para los tres flows del namespace `football.transfermarkt`. |
| cambio de transformación, ejecución `5PxTKhblpA3nKOsUTTuVaG` | `SUCCESS`, razón `transformation_changed`, 217 nodos: 206 pass/success, 11 warnings, 0 errores, un intento y 456 artifacts. |
| omisión `30RjosxG2mHU8GvO6Cq4Zx` | `SUCCESS` del flow; run auditado `SKIPPED`, razón `already_validated`, sin task dbt. |
| force `udCW4ajULEm66Br4MnR4Y` | `SUCCESS`, razón `force_requested`, mismo build completo, un intento y 456 artifacts; checkpoint avanzado solo al final. |
| ejecución simultánea `lAGGpFlBFYDNZojYsgYYh` | Se observó `QUEUED` mientras corría force; después terminó auditada `SKIPPED/already_validated`. |
| backfill inválido aislado `7hebnjC19EzpYHvC3uKowY` | `FAILED` en preflight con un único intento; no ejecutó dbt y el checkpoint siguió apuntando a `udCW4ajULEm66Br4MnR4Y`. |
| recuperación posterior `zz4KMBS2qv7KKn3v7ACdA` | Ejecución normal iniciada sin cambios Bronze; terminó sin reconstrucción pendiente y conservó el checkpoint válido. |
| force final `5mMSElah1W4vulbEyrKHQ1` | `SUCCESS`, 217 nodos, un intento, 456 artifacts y checkpoint final; confirmó las huellas estrictas de negocio antes/después. |

El fallo histórico real de Stage 7 (`SAzw3lVZUpHDo9w42MeoS`) y su recuperación
`6VVj0tYPDb1SrfZQmlllu9` ya demostraron el caso sin checkpoint: Silver falló,
Bronze no recibió archivos nuevos y la siguiente ejecución reconstruyó y validó
todo. En Stage 8 se añadió la prueba unitaria del camino posterior a un fallo
dbt: puede haber objetos materializados, pero `success=False` no borra ni
inserta checkpoint. El runner mantiene tres intentos totales solo para fallos
transitorios reconocidos y uno para calidad/autenticación/schema; la suite lo
verificó con fixtures.

### Datos, cobertura e idempotencia

Las huellas siguientes se midieron antes/después de reconstrucciones del mismo
manifiesto. Excluyen timestamps e IDs/checksums/versiones de procesamiento y
lineage (`*_processed_at`, `manifest_resolved_at`, `*_source_version*`,
`*_source_captured_at` y `*_bronze_ingestion_run_id`):

| Modelo | Filas = claves distintas | Duplicados | Huella de negocio |
|---|---:|---:|---:|
| `silver_players_current` | 50.149 | 0 | `1142327180427301953` |
| `silver_games_enriched` | 88.958 | 0 | `2784684833694492177` |
| `silver_player_match` | 1.894.350 | 0 | `-9156827921968501622` |
| `silver_player_valuations_enriched` | 656.301 | 0 | `-7509390351980322059` |

- Los 12 assets reconciliaron 7.497.433 entradas/aceptadas, cero duplicados,
  cero rechazos/conflictos y cero balances rotos; quarantine tiene cero filas.
- Los `LEFT JOIN` preservaron 50.149 jugadores pese a 46.889 sin selección,
  106 selecciones no encontradas y 2.986 clubes no encontrados. Nacimiento y
  ciudadanía resolvieron por separado; 3.372 jugadores tienen ambos roles
  resueltos a países distintos.
- Se preservaron 88.958 partidos pese a 12.818 locales, 11.242 visitantes y
  1.214 competiciones sin catálogo. Los nombres históricos `*_at_game` no se
  sustituyeron por atributos `*_current_snapshot`.
- Las 1.894.350 participaciones resolvieron game y el par
  (`game_id`, `player_club_id`): 938.329 home y 956.021 away, sin contexto
  inconsistente ni multiplicación.
- Las 656.301 valoraciones conservaron 41.528 jugadores; 40.465 tienen más de
  una fecha y el máximo es 57. Importes: cero nulos/negativos, un cero válido,
  €0..€200.000.000 y suma €1.502.990.157.999.
- Los 11 warnings son únicamente la cobertura de catálogos ya documentada;
  grain, conservación, manifiesto, importes, reconciliación y prueba conjunta
  pasaron como controles críticos.

### Comandos PowerShell comprobados

```powershell
docker compose config --quiet
docker compose --profile silver build dbt
docker build --target test -t transfermarkt-bronze-tests:stage8 .
docker run --rm transfermarkt-bronze-tests:stage8 -m pytest -q

docker compose build kestra
docker compose up -d --no-deps --force-recreate kestra
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\wait-kestra.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\register-flows.ps1

docker compose --profile silver run --rm dbt parse `
  --target test --no-version-check --no-partial-parse --warn-error
docker compose --profile silver run --rm dbt debug --target test --no-version-check
docker compose --profile silver run --rm dbt show `
  --target test --inline "select 1 as connection_ok" --no-version-check

powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run-silver-flow.ps1 -Target test
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\run-silver-flow.ps1 -Target test -Force

docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python `
  -m ingestion.silver audit --target test --limit 10
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python `
  -m ingestion.silver verify --target test
```

### Cierre y límites pendientes

Silver queda entregado y validado en `test`, no promovido a producción. Queda
pendiente de una decisión operativa, no de implementación Silver:

- ejecutar preflight/build en `prod` y habilitar el Schedule Bronze, que sigue
  `disabled: true`; los permisos comprobados en `test` no se extrapolan;
- probar un backfill alternativo solo si existe otra combinación histórica
  real conservada. Repetir fechas no crea snapshots y el intento inválido se
  rechazó correctamente;
- `source_version`/`source_captured_at` siguen `NULL` para auditoría Bronze
  legada que no los registró; no se inventaron;
- el Subflow Bronze → Silver está registrado y validado, pero no se relanzó
  Bronze solo para probarlo. Las ejecuciones manuales comprobaron el mismo flow
  Silver y su contenedor hijo;
- Gold, joins as-of, ventanas, etiqueta, features y modelo predictivo quedan
  explícitamente para otra tarea.
