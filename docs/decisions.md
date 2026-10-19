| Tabla | Campos | Regla Silver | Nota |
|---|---|---|---|
| **appearances** | `appearance_id` | `TRIM`; `NOT NULL`; `UNIQUE`. | Clave principal observada. |
| | `game_id`, `player_id` | Cast a `INTEGER`; `NOT NULL`. | Necesarios para el grain jugador-partido. |
| | `player_club_id` | Cast a `INTEGER`; nullable. | Club representado en ese partido. |
| | `player_current_club_id` | Cast a `INTEGER`; `-1` se queda como `-1`; nullable. | El se interpreta como no club. |
| | `date` | Cast a `DATE`; nullable. | Fecha de la aparición contenida en el game_id |
| | `player_name` | `TRIM`; nullable. | La identidad ya está dada por `player_id`; se observaron 2 nulos. |
| | `competition_id` | `UPPER(TRIM(...))`; nullable. | Código textual de competición. |
| | `yellow_cards`, `red_cards`, `goals`, `assists`, `minutes_played` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | No se autocompleta. |
| **club_games** | `game_id`, `club_id` | Cast a `INTEGER`; `NOT NULL`; combinación `(game_id, club_id)` `UNIQUE`. | Grain club-partido confirmado. |
| | `opponent_id` | Cast a `INTEGER`; `NOT NULL`. | Identifica al adversario. |
| | `own_goals`, `opponent_goals` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Marcadores. Si es `NULL` se deja como `NULL`|
| | `own_position`, `opponent_position` | Cast a `INTEGER`; nullable; si existe, `>= 1`. | El profiling mostró ~28.5% de nulos; son admisibles. |
| | `own_manager_name`, `opponent_manager_name` | `TRIM`; nullable. | Descriptivos. |
| | `hosting` | `LOWER(TRIM(...))`; nullable; valores admitidos `{home, away}`. | Dominio completo observado. |
| | `is_win` | Cast a `BOOLEAN`; `1 → TRUE`, `0 → FALSE`; nullable. | Si existen marcador de `is_win`, validar `is_win = (own_goals > opponent_goals)`. |
| **clubs** | `club_id` | Cast a `INTEGER`; `NOT NULL`; `UNIQUE`. | Clave principal. |
| | `club_code` | `LOWER(TRIM(...))`; `NOT NULL`; `UNIQUE`. | Código único en el snapshot. |
| | `name` | `TRIM`; `NOT NULL`. | Nombre de la entidad. |
| | `domestic_competition_id` | `UPPER(TRIM(...))`; nullable. | No hace falta |
| | `total_market_value` | Cast a `DECIMAL`; nullable; si existe, `>= 0`. | 796/796 valores estaban ausentes; conservar `NULL`, nunca convertirlo a cero. |
| | `squad_size` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Conteo. |
| | `average_age` | Cast a `DECIMAL`; nullable; si existe, `> 0`. | Simple |
| | `foreigners_number` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Conteo. |
| | `foreigners_percentage` | Cast a `DECIMAL`; nullable; si existe, entre `0` y `100`. | Porcentaje. |
| | `national_team_players` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Conteo. |
| | `stadium_name` | `TRIM`; nullable. | Descriptivo. |
| | `stadium_seats` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Capacidad. |
| | `net_transfer_record` | Parsear a `DECIMAL` en EUR; nullable; admitir valores positivos, cero y negativos. | El signo representa balance económico, no invalidez. |
| | `coach_name` | `TRIM`; nullable. | ~49% estaba ausente; no afecta la identidad del club. |
| | `last_season` | Cast a `INTEGER`; `NOT NULL`. | Año/temporada de referencia. |
| | `filename` | `TRIM`; `NOT NULL`. | Conservar como lineage de la fuente. |
| | `url` | `TRIM`; `NOT NULL`. | Referencia de la entidad. |
| **competitions** | `competition_id` | `UPPER(TRIM(...))`; `NOT NULL`; `UNIQUE`. | Clave alfanumérica. |
| | `competition_code` | `LOWER(TRIM(...))`; `NOT NULL`. | Código textual. |
| | `name` | `TRIM`; `NOT NULL`. | Nombre de la competición. |
| | `sub_type`, `type` | `LOWER(TRIM(...))`; `NOT NULL`. | Categorías estandarizadas. |
| | `country_id` | Cast a `INTEGER`; `-1` se queda como `-1`; nullable. | Competiciones internacionales pueden no pertenecer a un país. |
| | `country_name` | `TRIM`; nullable. | Mantener NULL cuando no aplica. |
| | `domestic_league_code` | `UPPER(TRIM(...))`; nullable. | No aplica a todas las competiciones. |
| | `confederation` | `LOWER(TRIM(...))`; `NOT NULL`. | Categoría estandarizada. |
| | `total_clubs` | Cast a `INTEGER`; nullable; si existe, `>= 0`. | Ausencia permitida para algunos tipos de competición. |
| | `url` | `TRIM`; `NOT NULL`. | Referencia. |
| **countries** | `country_id` | Cast a `INTEGER`; `NOT NULL`; `UNIQUE`. | Clave principal. |
| | `country_name` | `TRIM`; `NOT NULL`; `UNIQUE`. | Nombre único observado. |
| | `country_code` | `UPPER(TRIM(...))`; `NOT NULL`; `UNIQUE`. | Código estandarizado. |
| | `confederation` | `LOWER(TRIM(...))`; `NOT NULL`. | Dominio categórico homogéneo. |
| | `total_clubs`, `total_players` | Cast a `INTEGER`; nullable; `>= 0`. | Ambos completos en el snapshot. |
| | `average_age` | Cast a `DECIMAL`; nullable; `> 0`. | Completo y positivo en los datos observados.|
| | `url` | `TRIM`; `NOT NULL`. | Referencia. |
| **game_events** | `game_event_id` | `TRIM`; `NOT NULL`; `UNIQUE`. | Clave principal. |
| | `date` | Cast a `DATE`; `NOT NULL`. | Fecha del evento. |
| | `game_id` | Cast a `INTEGER`; `NOT NULL`. | Relación con `games`; el profiling dio 0 huérfanos. |
| | `minute` | Cast a `INTEGER`; `-1` se conserva como `-1`; `NOT NULL`; otros valores deben ser `>=0`. | `-1` se interpreta como el tiempo añadido porque el dataset no captura tiempos añadidos de forma explícita|
| | `type` | `LOWER(TRIM(...))`; `NOT NULL`. | Conservar las categorías observadas normalizando. |
| | `club_id`, `player_id`, `player_in_id`, `player_assist_id` | Cast a `INTEGER`; nullable. | Dependiendo del tipo de evento, varios no aplican. No se añade una validacion logica por tipo de accion porque no hay un dominio especificado del tipo de acciones posibles. |
| | `club_name`, `description` | `TRIM`; nullable. | Descriptivos. |
| **game_lineups** | `game_lineups_id` | `TRIM`; `NOT NULL`; `UNIQUE`. | Clave principal observada. |
| | `date` | Cast a `DATE`; `NOT NULL`. | Fecha de la alineación. |
| | `game_id`, `player_id`, `club_id` | Cast a `INTEGER`; `NOT NULL`. | Necesarios para interpretar la participación. |
| | `player_name` | `TRIM`; nullable. | Identidad disponible mediante `player_id`. |
| | `type` | `LOWER(TRIM(...))`; `NOT NULL`; valores admitidos según dominio observado. | Mantener `starting_lineup`, `substitutes`, etc. sin inventar categorías nuevas. |
| | `position` | `TRIM`; nullable. | Descriptivo. |
| | `number` | `"-" → NULL`; luego cast a `INTEGER`; nullable. | Se observaron 25,698 placeholders `"-"`. |
| | `team_captain` | Cast a `BOOLEAN`; `0 → FALSE`, `1 → TRUE`; nullable. | Dominio booleano observado. |
| **games** | `game_id` | Cast a `INTEGER`; `NOT NULL`; `UNIQUE`. | Grain partido. |
| | `competition_id` | `UPPER(TRIM(...))`; `NOT NULL`. | No exigir FK estricta: existen códigos históricos fuera de `competitions`. |
| | `season` | Cast a `INTEGER`; `NOT NULL`. | Temporada. |
| | `round` | `TRIM`; `NOT NULL`. | Categoría/ronda textual. |
| | `date` | Cast a `DATE`; `NOT NULL`. | Fecha del partido. |
| | `home_club_id`, `away_club_id` | Cast a `INTEGER`; `NOT NULL`; exigir `home_club_id <> away_club_id`. | Regla interna del partido.|
| | `home_club_goals`, `away_club_goals` | Cast a `INTEGER`; nullable; si existe, `>=0`. | No autocompletar. |
| | `home_club_position`, `away_club_position` | Cast a `INTEGER`; nullable; si existe, `>=1`. | Solo aplica cuando existe posición de liga. |
| | `home_club_manager_name`, `away_club_manager_name` | `TRIM`; nullable. | Descriptivos. |
| | `stadium` | `TRIM`; nullable. | Descriptivo. |
| | `attendance` | Cast a `INTEGER`; nullable; si existe, `>=0`. | Conteo. |
| | `referee` | `TRIM`; nullable. | Descriptivo. |
| | `url` | `TRIM`; `NOT NULL`. | Referencia. |
| | `home_club_formation`, `away_club_formation` | `TRIM`; nullable. | Descriptivos. |
| | `home_club_name`, `away_club_name` | `TRIM`; nullable. | Los IDs mantienen la identidad. |
| | `aggregate` | `TRIM`; nullable. | Mantener como texto fuente. |
| | `competition_type` | `LOWER(TRIM(...))`; nullable. | Categórico. |
| **national_teams** | `national_team_id` | Cast a `INTEGER`; `NOT NULL`; `UNIQUE`. | Clave principal. |
| | `name` | `TRIM`; `NOT NULL`. | Nombre de la selección. |
| | `team_code` | `LOWER(TRIM(...))`; `NOT NULL`. | Código fuente. |
| | `country_id` | Cast a `INTEGER`; `NOT NULL`; FK a `countries.country_id`. | El profiling encontró 0 huérfanos. |
| | `country_name` | `TRIM`; `NOT NULL`. | Descriptivo. |
| | `country_code` | `UPPER(TRIM(...))`; `NOT NULL`. | Código estandarizado. |
| | `confederation` | `UPPER(TRIM(...))`; `NOT NULL`. | La fuente de selecciones usa categorías como `UEFA`. |
| | `team_image_url` | `TRIM`; nullable. | Descriptivo. |
| | `squad_size` | Cast a `INTEGER`; nullable; si existe, `>=0`. | Conteo. |
| | `average_age` | Cast a `DECIMAL`; nullable; si existe, `>0`. | Edad media. |
| | `foreigners_number` | Cast a `INTEGER`; nullable; si existe, `>=0`. | Conteo. |
| | `foreigners_percentage` | Cast a `DECIMAL`; nullable; si existe, `[0,100]`. | Porcentaje. |
| | `total_market_value` | Cast a `DECIMAL`; nullable; si existe, `>=0`. | No imputar ausencia con cero. |
| | `coach_name` | `TRIM`; nullable. | Descriptivo. |
| | `fifa_ranking` | Cast a `INTEGER`; nullable; si existe, `>=1`. | Ranking. |
| | `last_season` | Cast a `INTEGER`; `NOT NULL`. | Temporada. |
| | `url` | `TRIM`; `NOT NULL`. | Referencia. |
| **player_valuations** | `player_id` | Cast a `INTEGER`; `NOT NULL`; FK a `players.player_id`. | El profiling encontró 0 huérfanos. |
| | `date` | Cast a `DATE`; `NOT NULL`. | Fecha de valoración. |
| | `market_value_in_eur` | Cast a `DECIMAL`; `NOT NULL`; `>=0`. | Es la medida central de esta tabla y del target del proyecto.|
| | `current_club_name` | `TRIM`; `NULL → "Unknown"`; conservar `"Without Club"`. | `Unknown` expresa dato desconocido; `Without Club` expresa un estado real. |
| | `current_club_id` | Cast a `INTEGER`; nullable. | No exigir porque puede estar libre. |
| | `player_club_domestic_competition_id` | `UPPER(TRIM(...))`; nullable. | Ausencia permitida. |
| | **grain** | `(player_id, date)` `UNIQUE`. | Grain confirmado por el profiling. |
| **players** | `player_id` | Cast a `INTEGER`; `NOT NULL`; `UNIQUE`. | Clave principal. |
| | `first_name`, `last_name` | `TRIM`; nullable. | Algunos jugadores pueden no disponer de ambas partes del nombre. |
| | `name` | `TRIM`; `NOT NULL`. | Nombre descriptivo principal. |
| | `last_season` | Cast a `INTEGER`; `NOT NULL`. | Temporada de referencia. |
| | `current_club_id` | Cast a `INTEGER`; `-1` se queda como eitqueta de no club, que no haya NULLs en esa columna nos anuncia que es su indicador; nullable. | Un jugador puede no tener club actual. |
| | `player_code` | `LOWER(TRIM(...))`; `NOT NULL`. | Código textual. |
| | `country_of_birth`, `city_of_birth`, `country_of_citizenship` | `TRIM`; nullable. | Atributos descriptivos opcionales. |
| | `date_of_birth` | Cast a `DATE`; nullable; si existe, `<= fecha de corte`. | Una fecha futura es inválida. |
| | `sub_position`, `position` | `TRIM`; nullable. | Categorías descriptivas.|
| | `foot` | `LOWER(TRIM(...))`; nullable; valores admitidos `{left, right, both}`. | Dominio categórico.|
| | `height_in_cm` | Cast a `INTEGER`; nullable; si existe, `>0`. | Evitar rangos antropométricos arbitrarios. |
| | `contract_expiration_date` | Cast a `DATE`; nullable; si existen ambas fechas, debe ser `>= date_of_birth`. | Control básico de consistencia temporal. |
| | `agent_name`, `image_url` | `TRIM`; nullable. | Descriptivos. |
| | `international_caps`, `international_goals` | Cast a `INTEGER`; si `NULL` se queda `NULL`; si existe, `>=0`. | Conteos. |
| | `current_national_team_id` | Cast a `INTEGER`; `-1 → NULL`; nullable. | No todo jugador pertenece actualmente a una selección. |
| | `url` | `TRIM`; `NOT NULL`. | Referencia. |
| | `current_club_domestic_competition_id` | `UPPER(TRIM(...))`; nullable. | Depende de la existencia de club actual. |
| | `current_club_name` | `TRIM`; nullable. | Depende de la existencia de club actual. |
| | `market_value_in_eur`, `highest_market_value_in_eur` | Cast a `DECIMAL`; nullable; si existe, `>=0`. | No imputar cero. La historia temporal se obtiene de `player_valuations`.|
| **transfers** | `player_id` | Cast a `INTEGER`; `NOT NULL`; FK a `players.player_id`. | El profiling encontró 0 huérfanos. |
| | `transfer_date` | Cast a `DATE`; `NOT NULL`. | Fecha del evento. Mantener fechas futuras si son válidas en la fuente. |
| | `transfer_season` | `TRIM`; `NOT NULL`. | Mantener como `VARCHAR` por valores tipo `25/26`. |
| | `from_club_id`, `to_club_id` | Cast a `INTEGER`; nullable. | Movimientos desde/hacia “sin club” pueden no tener ID. No exigir FK a `clubs`, pero ambos no pueden ser `NULL` a la vez. |
| | `from_club_name`, `to_club_name` | `TRIM`; nullable. | Descriptivos. |
| | `transfer_fee` | Cast a `DECIMAL`; nullable; si existe, `>=0`. | NULL significa tarifa desconocida/no informada; no convertir a cero. |
| | `market_value_in_eur` | Cast a `DECIMAL`; nullable; si existe, `>=0`. | No imputar.|
| | `player_name` | `TRIM`; nullable. | Identidad mediante `player_id`. |
| | **grain** | `(player_id, transfer_date, from_club_id, to_club_id)` `UNIQUE`. | Combinación única en el snapshot; no crear un `transfer_id` artificial. |


| Tipo de incumplimiento | Acción |
|---|---|
| Campo `nullable` ausente | Mantener `NULL`. La fila entra a Silver. |
| Campo `nullable` presente pero no parseable o fuera de su dominio | Convertir **ese campo a `NULL`**. La fila entra a Silver. |
| Campo `NOT NULL` ausente o que no puede convertirse al tipo requerido | **Rechazar fila** de Silver principal. |
| Clave/grain duplicado, filas completamente idénticas | Conservar una sola fila de forma determinista. |
| Clave/grain duplicado con atributos diferentes | **Rechazar las filas conflictivas**; no escoger arbitrariamente una. |
| Regla estructural esencial entre campos incumplida | **Rechazar fila**. |
| FK marcada explícitamente como obligatoria | Si no existe padre, **rechazar fila**. |
| FK no exigida explícitamente | Mantener el ID aunque no exista en la dimensión actual. |
| Normalización conocida (`"-"→NULL`, casing, sentinel conservado, etc.) | Aplicar la transformación decidida; no es un rechazo. |
