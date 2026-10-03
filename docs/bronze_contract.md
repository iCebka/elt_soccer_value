# Contrato Bronze para Silver y dbt

## Objetos

El database y schema son configurables; los valores propuestos son `FOOTBALL.BRONZE`.

| Asset | Tabla con todas las versiones | Vista de última versión exitosa |
|---|---|---|
| competitions | `COMPETITIONS_RAW` | `COMPETITIONS_LATEST` |
| clubs | `CLUBS_RAW` | `CLUBS_LATEST` |
| players | `PLAYERS_RAW` | `PLAYERS_LATEST` |
| games | `GAMES_RAW` | `GAMES_LATEST` |
| appearances | `APPEARANCES_RAW` | `APPEARANCES_LATEST` |
| player_valuations | `PLAYER_VALUATIONS_RAW` | `PLAYER_VALUATIONS_LATEST` |
| transfers | `TRANSFERS_RAW` | `TRANSFERS_LATEST` |
| club_games | `CLUB_GAMES_RAW` | `CLUB_GAMES_LATEST` |
| game_events | `GAME_EVENTS_RAW` | `GAME_EVENTS_LATEST` |
| game_lineups | `GAME_LINEUPS_RAW` | `GAME_LINEUPS_LATEST` |
| countries | `COUNTRIES_RAW` | `COUNTRIES_LATEST` |
| national_teams | `NATIONAL_TEAMS_RAW` | `NATIONAL_TEAMS_LATEST` |

También se entregan `TRANSFERMARKT_INGESTION_BATCHES`, `TRANSFERMARKT_INGESTION_FILES`, el stage interno `TRANSFERMARKT_BRONZE_STAGE` y el file format `TRANSFERMARKT_NDJSON_FORMAT`.

## Grain, metadata y versionado

El grain de cada tabla RAW es una fila original del CSV dentro de una versión identificada por URL y `SOURCE_FILE_SHA256`. El grain de cada vista `*_LATEST` es una fila original del CSV perteneciente al intento `SUCCESS` más reciente del asset (`FINISHED_AT`, luego `RUN_ID`). Los intentos `FAILED` y `SKIPPED_UNCHANGED` no seleccionan una versión RAW distinta: el skip referencia un `SUCCESS` anterior.

Cada tabla RAW contiene:

- `RAW_RECORD VARIANT`: objeto con cada encabezado fuente y su valor textual exacto. Se conservan IDs, Unicode y cadenas vacías; no se tipan importes o fechas.
- `SOURCE_URL`, `SOURCE_FILE`, `SOURCE_FILE_SHA256`.
- `SOURCE_ROW_NUMBER`: posición lógica del registro CSV, empezando en 1 después del encabezado. No es la línea física: un campo puede contener saltos de línea.
- `INGESTION_RUN_ID` y `LOADED_AT` UTC.

Los encabezados observados se guardan como array en `TRANSFERMARKT_INGESTION_FILES.OBSERVED_HEADERS`. `SCHEMA_CHANGED` indica una diferencia frente al contrato observado en `config/assets.yml`; ninguna columna nueva se descarta porque el objeto completo se conserva.

La auditoría de archivos incluye `DATASET`, `SOURCE_URL`, fecha lógica, consulta efectiva, captura, estados HTTP, ETag remoto y de descarga, `Last-Modified`, `Content-Length`, SHA-256, número de intentos, filas, referencia usada, estado, motivo de skip y error. ETag es opaco y puede conservar comillas. `Last-Modified` o tamaño por sí solos no identifican una versión.

## Encabezados observados

- `competitions`: `competition_id`, `competition_code`, `name`, `sub_type`, `type`, `country_id`, `country_name`, `domestic_league_code`, `confederation`, `total_clubs`, `url`.
- `clubs`: `club_id`, `club_code`, `name`, `domestic_competition_id`, `total_market_value`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `national_team_players`, `stadium_name`, `stadium_seats`, `net_transfer_record`, `coach_name`, `last_season`, `filename`, `url`.
- `players`: `player_id`, `first_name`, `last_name`, `name`, `last_season`, `current_club_id`, `player_code`, `country_of_birth`, `city_of_birth`, `country_of_citizenship`, `date_of_birth`, `sub_position`, `position`, `foot`, `height_in_cm`, `contract_expiration_date`, `agent_name`, `image_url`, `international_caps`, `international_goals`, `current_national_team_id`, `url`, `current_club_domestic_competition_id`, `current_club_name`, `market_value_in_eur`, `highest_market_value_in_eur`.
- `games`: `game_id`, `competition_id`, `season`, `round`, `date`, `home_club_id`, `away_club_id`, `home_club_goals`, `away_club_goals`, `home_club_position`, `away_club_position`, `home_club_manager_name`, `away_club_manager_name`, `stadium`, `attendance`, `referee`, `url`, `home_club_formation`, `away_club_formation`, `home_club_name`, `away_club_name`, `aggregate`, `competition_type`.
- `appearances`: `appearance_id`, `game_id`, `player_id`, `player_club_id`, `player_current_club_id`, `date`, `player_name`, `competition_id`, `yellow_cards`, `red_cards`, `goals`, `assists`, `minutes_played`.
- `player_valuations`: `player_id`, `date`, `market_value_in_eur`, `current_club_name`, `current_club_id`, `player_club_domestic_competition_id`.
- `transfers`: `player_id`, `transfer_date`, `transfer_season`, `from_club_id`, `to_club_id`, `from_club_name`, `to_club_name`, `transfer_fee`, `market_value_in_eur`, `player_name`.
- `club_games`: `game_id`, `club_id`, `own_goals`, `own_position`, `own_manager_name`, `opponent_id`, `opponent_goals`, `opponent_position`, `opponent_manager_name`, `hosting`, `is_win`.
- `game_events`: `game_event_id`, `date`, `game_id`, `minute`, `type`, `club_id`, `club_name`, `player_id`, `description`, `player_in_id`, `player_assist_id`.
- `game_lineups`: `game_lineups_id`, `date`, `game_id`, `player_id`, `club_id`, `player_name`, `type`, `position`, `number`, `team_captain`.
- `countries`: `country_id`, `country_name`, `country_code`, `confederation`, `total_clubs`, `total_players`, `average_age`, `url`.
- `national_teams`: `national_team_id`, `name`, `team_code`, `country_id`, `country_name`, `country_code`, `confederation`, `team_image_url`, `squad_size`, `average_age`, `foreigners_number`, `foreigners_percentage`, `total_market_value`, `coach_name`, `fifa_ranking`, `last_season`, `url`.

## Ejemplos SQL

```sql
-- IDs conservados para joins, fecha de negocio y valor de mercado.
SELECT
  RAW_RECORD:player_id::NUMBER                 AS player_id,
  TRY_TO_DATE(NULLIF(RAW_RECORD:date::VARCHAR, '')) AS valuation_date,
  TRY_TO_NUMBER(NULLIF(RAW_RECORD:market_value_in_eur::VARCHAR, '')) AS market_value_eur,
  LOADED_AT                                   AS bronze_loaded_at
FROM FOOTBALL.BRONZE.PLAYER_VALUATIONS_LATEST;

-- Fecha de partido; no confundir con LOADED_AT.
SELECT
  RAW_RECORD:game_id::NUMBER AS game_id,
  TRY_TO_DATE(NULLIF(RAW_RECORD:date::VARCHAR, '')) AS match_date
FROM FOOTBALL.BRONZE.GAMES_LATEST;
```

`LOADED_AT` responde “cuándo se publicó esta fila en Bronze”. `player_valuations.date` es la fecha de la valoración y `games.date`/`appearances.date` son fechas de negocio. No use `LOADED_AT` para afirmar que el dato era conocido en una fecha histórica de predicción; el proveedor distribuye un snapshot actual y no versiones point-in-time pasadas.

## Comprobar un lote

```sql
SELECT BATCH_ID, LOGICAL_DATE, TRIGGER_SOURCE, STATUS,
       REQUESTED_ASSETS, SUMMARY, ERROR
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_BATCHES
WHERE BATCH_ID = '<kestra-execution-id>';

SELECT DATASET, ASSET, SOURCE_URL, STATUS, LOGICAL_DATE,
       SOURCE_CHECKED_AT, CAPTURED_AT, REMOTE_ETAG, DOWNLOAD_ETAG,
       SOURCE_FILE_SHA256, HTTP_ATTEMPTS, REFERENCE_RUN_ID,
       ROWS_READ, ROWS_LOADED, ERROR
FROM FOOTBALL.BRONZE.TRANSFERMARKT_INGESTION_FILES
WHERE BATCH_ID = '<kestra-execution-id>'
ORDER BY ASSET, STARTED_AT;
```

El lote es utilizable únicamente cuando la fila de `TRANSFERMARKT_INGESTION_BATCHES` tiene `STATUS='SUCCESS'`; todos los assets pedidos tendrán un intento `SUCCESS` o `SKIPPED_UNCHANGED`.

## Sources de dbt

```yaml
version: 2
sources:
  - name: transfermarkt_bronze
    database: "{{ env_var('SNOWFLAKE_DATABASE', 'FOOTBALL') }}"
    schema: "{{ env_var('SNOWFLAKE_BRONZE_SCHEMA', 'BRONZE') }}"
    loader: kestra_transfermarkt_bronze
    tables:
      - name: competitions_latest
      - name: clubs_latest
      - name: players_latest
      - name: games_latest
      - name: appearances_latest
      - name: player_valuations_latest
      - name: transfers_latest
      - name: club_games_latest
      - name: game_events_latest
      - name: game_lineups_latest
      - name: countries_latest
      - name: national_teams_latest
      - name: transfermarkt_ingestion_batches
      - name: transfermarkt_ingestion_files
```

