# OBT — Decisiones de la tabla de eventos

## 1. Principios generales

La OBT se construye con Spark a partir de las cuatro tablas de hechos y las dimensiones de Gold.

La tabla final se denomina:

```text
obt_football_events
```

La OBT no reemplaza al modelo dimensional. Gold conserva cada proceso en su grain natural; la OBT proyecta esos procesos sobre una interfaz temporal común y deliberadamente desnormalizada.

Se aplican las siguientes reglas generales:

- La OBT consume únicamente datos ya aceptados en Gold.
- La OBT no aplica una segunda política de limpieza ni vuelve a decidir la calidad de las filas fuente.
- Cada fila Gold elegible debe producir exactamente una fila en la OBT.
- El **grain de la OBT** es una observación temporal del dominio correspondiente a uno de los grains analíticos definidos en Gold.
- Los cuatro tipos de observación son `PLAYER_MATCH`, `MATCH_ACTION`, `TRANSFER` y `VALUATION`.
- `event_type` identifica qué grain original representa la fila.
- Los atributos que no aplican al tipo de evento permanecen `NULL`.
- Los joins con dimensiones se utilizan únicamente para denormalizar contexto descriptivo; nunca pueden multiplicar observaciones.
- La ausencia de una dimensión opcional no elimina un evento.
- La OBT conserva granularidad histórica; no calcula todavía ventanas móviles ni el target del futuro modelo.
- La salida de Spark se escribe nuevamente en Snowflake.

---

## 2. Grain y tipos de evento

**Grain general:** una observación temporal del dominio representada por uno de los grains Gold.

| `event_type` | Fact Gold de origen | Grain original |
|---|---|---|
| `PLAYER_MATCH` | `fact_player_match` | Participación de un jugador en un partido: `(game_id, player_id)` |
| `MATCH_ACTION` | `fact_match_event` | Acción registrada dentro de un partido: `game_event_id` |
| `TRANSFER` | `fact_transfer` | Movimiento de un jugador: `(player_id, transfer_date, from_club_id, to_club_id)` |
| `VALUATION` | `fact_player_valuation` | Valoración de un jugador: `(player_id, valuation_date)` |

Un evento de la OBT no tiene que ser atómico. Por ejemplo, `PLAYER_MATCH` representa una participación completa jugador-partido y agrupa medidas como minutos, goles y asistencias. Sigue siendo una observación válida porque posee un grain definido y estable.

---

## 3. Identidad y lineage del evento

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `event_id` | `VARCHAR(64)` | `NOT NULL`; `UNIQUE` | SHA-256 determinista construido con `event_type` y la clave natural del grain original. |
| `event_type` | `VARCHAR` | `NOT NULL`; dominio `{PLAYER_MATCH, MATCH_ACTION, TRANSFER, VALUATION}` | Identifica la semántica y el grain de la fila. |
| `event_subtype` | `VARCHAR` | Nullable | Para `MATCH_ACTION` contiene el tipo de acción de partido. En los otros tipos permanece `NULL`. |
| `source_fact` | `VARCHAR` | `NOT NULL` | Modelo Gold que originó la observación. |
| `source_record_id` | `VARCHAR` | Nullable | Identificador fuente cuando existe uno directo, por ejemplo `appearance_id` o `game_event_id`. |
| `source_key` | `VARCHAR` | `NOT NULL` | Representación legible y determinista de la clave natural del evento. |

### 3.1. Construcción de `event_id`

El identificador debe ser estable entre ejecuciones.

```text
PLAYER_MATCH
hash("PLAYER_MATCH", game_id, player_id)

MATCH_ACTION
hash("MATCH_ACTION", game_event_id)

TRANSFER
hash(
    "TRANSFER",
    player_id,
    transfer_date,
    COALESCE(from_club_id, "<NULL>"),
    COALESCE(to_club_id, "<NULL>")
)

VALUATION
hash("VALUATION", player_id, valuation_date)
```

El mismo tratamiento `NULL`-safe utilizado para `TRANSFER` debe aplicarse también a `source_key`.

### 3.2. `source_record_id`

```text
PLAYER_MATCH  -> appearance_id
MATCH_ACTION  -> game_event_id
TRANSFER      -> NULL
VALUATION     -> NULL
```

`source_record_id` no sustituye a `event_id`; se conserva únicamente para trazabilidad.

---

## 4. Contexto temporal común

Todas las filas de la OBT se proyectan sobre una fecha común denominada `event_date`.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `event_date_key` | `INTEGER` | `NOT NULL`; relación obligatoria con `dim_date.date_key` | Clave calendario `YYYYMMDD`. |
| `event_date` | `DATE` | `NOT NULL` | Fecha efectiva de la observación. |
| `event_year` | `INTEGER` | `NOT NULL` | Derivado de `dim_date`. |
| `event_quarter` | `INTEGER` | `NOT NULL`; `[1,4]` | Derivado de `dim_date`. |
| `event_month` | `INTEGER` | `NOT NULL`; `[1,12]` | Derivado de `dim_date`. |
| `event_month_name` | `VARCHAR` | `NOT NULL` | Derivado de `dim_date`. |
| `event_day` | `INTEGER` | `NOT NULL` | Día del mes. |
| `event_day_of_week` | `INTEGER` | `NOT NULL` | Día de la semana. |
| `event_day_name` | `VARCHAR` | `NOT NULL` | Nombre del día. |
| `event_week_of_year` | `INTEGER` | `NOT NULL` | Semana calendario. |

### 4.1. Mapeo de fechas

```text
PLAYER_MATCH
event_date     <- fact_player_match.game_date
event_date_key <- fact_player_match.game_date_key

MATCH_ACTION
event_date     <- fact_match_event.event_date
event_date_key <- fact_match_event.event_date_key

TRANSFER
event_date     <- fact_transfer.transfer_date
event_date_key <- fact_transfer.transfer_date_key

VALUATION
event_date     <- fact_player_valuation.valuation_date
event_date_key <- fact_player_valuation.valuation_date_key
```

Gold ya garantiza la existencia de estas fechas. Si una fila no puede asociarse a `dim_date`, la construcción de la OBT debe fallar en lugar de eliminar silenciosamente la observación.

---

## 5. Contexto del jugador

Los atributos del jugador se obtienen de `dim_player` mediante un `LEFT JOIN` por `player_id`.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `player_id` | `INTEGER` | Nullable a nivel OBT | Es obligatorio en `PLAYER_MATCH`, `TRANSFER` y `VALUATION`; puede ser `NULL` en `MATCH_ACTION`. |
| `player_code` | `VARCHAR` | Nullable | Código descriptivo del jugador. |
| `player_name` | `VARCHAR` | Nullable | `dim_player.name`. |
| `player_first_name` | `VARCHAR` | Nullable | Descriptivo. |
| `player_last_name` | `VARCHAR` | Nullable | Descriptivo. |
| `player_date_of_birth` | `DATE` | Nullable a nivel OBT | Disponible cuando `player_id` está cubierto por `dim_player`. |
| `player_country_of_birth` | `VARCHAR` | Nullable | Contexto demográfico. |
| `player_city_of_birth` | `VARCHAR` | Nullable | Contexto demográfico. |
| `player_country_of_citizenship` | `VARCHAR` | Nullable | Contexto demográfico. |
| `player_position` | `VARCHAR` | Nullable | Posición del jugador. |
| `player_sub_position` | `VARCHAR` | Nullable | Subposición del jugador. |
| `player_foot` | `VARCHAR` | Nullable | Pie dominante. |
| `player_height_in_cm` | `INTEGER` | Nullable | Altura. |
| `contract_expiration_date` | `DATE` | Nullable | Estado contractual disponible en el snapshot de `dim_player`. |

`contract_expiration_date` se conserva porque es necesario para el futuro cálculo de `months_remaining_on_contract`. Sin embargo, no se interpreta como un atributo históricamente válido para eventos anteriores al snapshot cuando no exista evidencia point-in-time del contrato.

### 5.1. Jugadores relacionados en acciones de partido

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `player_assist_id` | `INTEGER` | Nullable | Aplica a determinados `MATCH_ACTION`. |
| `player_assist_name` | `VARCHAR` | Nullable | Enriquecimiento opcional mediante `dim_player`. |
| `player_in_id` | `INTEGER` | Nullable | Aplica principalmente a sustituciones. |
| `player_in_name` | `VARCHAR` | Nullable | Enriquecimiento opcional mediante `dim_player`. |

Estos campos permiten filtrar acciones por participantes adicionales sin cambiar el grain del evento.

---

## 6. Contexto de club

Las dimensiones de club son descriptivas y su cobertura histórica no es completa. Todos los joins hacia `dim_club` son opcionales y no eliminan observaciones.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `club_id` | `INTEGER` | Nullable | Club principal del evento cuando la semántica lo permite. |
| `club_name` | `VARCHAR` | Nullable | Nombre obtenido de `dim_club`. |
| `opponent_club_id` | `INTEGER` | Nullable | Rival en `PLAYER_MATCH`. |
| `opponent_club_name` | `VARCHAR` | Nullable | Enriquecimiento opcional. |
| `from_club_id` | `INTEGER` | Nullable | Club de origen en `TRANSFER`. |
| `from_club_name` | `VARCHAR` | Nullable | Enriquecimiento opcional. |
| `to_club_id` | `INTEGER` | Nullable | Club de destino en `TRANSFER`. |
| `to_club_name` | `VARCHAR` | Nullable | Enriquecimiento opcional. |

### 6.1. Semántica de `club_id`

```text
PLAYER_MATCH  -> club representado por el jugador
MATCH_ACTION  -> club asociado a la acción, si existe
VALUATION     -> club registrado en la valoración, si existe
TRANSFER      -> NULL
```

Para `TRANSFER` no se copia arbitrariamente `from_club_id` o `to_club_id` a `club_id`, ya que ambos cumplen roles distintos.

---

## 7. Contexto de competición

La competición se enriquece con `dim_competition` cuando `competition_id` está disponible.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `competition_id` | `VARCHAR` | Nullable | No todos los eventos tienen competición asociada. |
| `competition_name` | `VARCHAR` | Nullable | `dim_competition.name`. |
| `competition_type` | `VARCHAR` | Nullable | Tipo de competición. |
| `competition_sub_type` | `VARCHAR` | Nullable | Subtipo de competición. |
| `competition_country_id` | `INTEGER` | Nullable | País de la competición cuando aplica. |
| `competition_country_name` | `VARCHAR` | Nullable | País de la competición. |
| `competition_confederation` | `VARCHAR` | Nullable | Confederación. |

La ausencia de una fila correspondiente en `dim_competition` no elimina el evento.

---

## 8. Campos de partido

### 8.1. Campos comunes de contexto de partido

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `game_id` | `INTEGER` | Nullable a nivel OBT | Presente en `PLAYER_MATCH` y `MATCH_ACTION`. |
| `minute` | `INTEGER` | Nullable; `-1` se conserva | Solo aplica a `MATCH_ACTION`. |

### 8.2. Participación jugador-partido

Los siguientes campos solo se pueblan para `event_type = PLAYER_MATCH`.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `home_or_away` | `VARCHAR` | Nullable; `{home, away}` | Perspectiva del club del jugador. |
| `is_win` | `BOOLEAN` | Nullable | Resultado desde la perspectiva del jugador/club. |
| `is_starter` | `BOOLEAN` | Nullable | Titularidad conocida. |
| `is_captain` | `BOOLEAN` | Nullable | Capitanía conocida. |
| `minutes_played` | `INTEGER` | Nullable; `>= 0` | Minutos disputados. |
| `goals` | `INTEGER` | Nullable; `>= 0` | Goles en la participación. |
| `assists` | `INTEGER` | Nullable; `>= 0` | Asistencias. |
| `yellow_cards` | `INTEGER` | Nullable; `>= 0` | Tarjetas amarillas. |
| `red_cards` | `INTEGER` | Nullable; `>= 0` | Tarjetas rojas. |

### 8.3. Acción puntual de partido

Los siguientes campos se pueblan para `event_type = MATCH_ACTION`.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `event_subtype` | `VARCHAR` | `NOT NULL` para `MATCH_ACTION` | Proviene de `fact_match_event.event_type`. |
| `minute` | `INTEGER` | `NOT NULL` para `MATCH_ACTION`; `-1` permitido | Convención heredada de Gold/Silver. |
| `action_description` | `VARCHAR` | Nullable | Descripción fuente de la acción. |
| `player_assist_id` | `INTEGER` | Nullable | Rol adicional cuando aplica. |
| `player_in_id` | `INTEGER` | Nullable | Jugador entrante cuando aplica. |

---

## 9. Campos de transferencia

Los siguientes campos se pueblan para `event_type = TRANSFER`.

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `transfer_season` | `VARCHAR` | `NOT NULL` para `TRANSFER` | Temporada textual, por ejemplo `25/26`. |
| `from_club_id` | `INTEGER` | Nullable individualmente | Al menos uno entre origen y destino existe por contrato Gold. |
| `from_club_name` | `VARCHAR` | Nullable | Enriquecimiento descriptivo. |
| `to_club_id` | `INTEGER` | Nullable individualmente | Al menos uno entre origen y destino existe por contrato Gold. |
| `to_club_name` | `VARCHAR` | Nullable | Enriquecimiento descriptivo. |
| `transfer_fee` | `DECIMAL` | Nullable; `>= 0` cuando exista | `NULL` no se interpreta como cero. |
| `market_value_in_eur` | `DECIMAL` | Nullable; `>= 0` cuando exista | Valor del jugador registrado en el movimiento. |

---

## 10. Campos de valoración

Para `event_type = VALUATION`:

| Campo | Tipo OBT | Regla | Nota |
|---|---|---|---|
| `market_value_in_eur` | `DECIMAL` | `NOT NULL`; `>= 0` | Valor de mercado observado. |
| `club_id` | `INTEGER` | Nullable | Club asociado a la valoración. |
| `competition_id` | `VARCHAR` | Nullable | Competición doméstica asociada, cuando existe. |

`market_value_in_eur` se reutiliza también para `TRANSFER` porque en ambos casos representa un valor de mercado expresado en EUR. Su interpretación concreta se determina mediante `event_type`.

---

## 11. Matriz de población por tipo de evento

`X` indica que el campo pertenece naturalmente al tipo de evento. `O` indica que es opcional. `—` indica que no aplica y debe permanecer `NULL`.

| Grupo / campo | `PLAYER_MATCH` | `MATCH_ACTION` | `TRANSFER` | `VALUATION` |
|---|:---:|:---:|:---:|:---:|
| `event_id`, `event_type`, `source_fact`, `source_key` | X | X | X | X |
| `event_date`, `event_date_key` | X | X | X | X |
| atributos de `dim_date` | X | X | X | X |
| `player_id` | X | O | X | X |
| atributos de `dim_player` | X | O | X | X |
| `contract_expiration_date` | O | O | O | O |
| `club_id` | O | O | — | O |
| `opponent_club_id` | O | — | — | — |
| `from_club_id`, `to_club_id` | — | — | O | — |
| `competition_id` | O | O | — | O |
| `game_id` | X | X | — | — |
| `event_subtype` | — | X | — | — |
| `minute` | — | X | — | — |
| participación: minutos/goles/asistencias/tarjetas | O | — | — | — |
| `is_starter`, `is_captain`, `is_win` | O | — | — | — |
| `player_assist_id`, `player_in_id` | — | O | — | — |
| `action_description` | — | O | — | — |
| `transfer_season` | — | — | X | — |
| `transfer_fee` | — | — | O | — |
| `market_value_in_eur` | — | — | O | X |

---

## 12. Proyección de cada fact Gold

### 12.1. `PLAYER_MATCH`

Origen:

```text
fact_player_match
```

Mapeo principal:

```text
event_type        = "PLAYER_MATCH"
event_subtype     = NULL
event_date        = game_date
event_date_key    = game_date_key
source_fact       = "fact_player_match"
source_record_id  = appearance_id
source_key        = (game_id, player_id)

player_id         = player_id
club_id           = club_id
opponent_club_id  = opponent_club_id
competition_id    = competition_id
game_id           = game_id

home_or_away      = home_or_away
is_win            = is_win
is_starter        = is_starter
is_captain        = is_captain
minutes_played    = minutes_played
goals             = goals
assists           = assists
yellow_cards      = yellow_cards
red_cards         = red_cards
```

### 12.2. `MATCH_ACTION`

Origen:

```text
fact_match_event
```

Mapeo principal:

```text
event_type        = "MATCH_ACTION"
event_subtype     = fact_match_event.event_type
event_date        = event_date
event_date_key    = event_date_key
source_fact       = "fact_match_event"
source_record_id  = game_event_id
source_key        = game_event_id

player_id         = player_id
player_assist_id  = player_assist_id
player_in_id      = player_in_id
club_id           = club_id
competition_id    = competition_id
game_id           = game_id
minute            = minute
action_description = description
```

### 12.3. `TRANSFER`

Origen:

```text
fact_transfer
```

Mapeo principal:

```text
event_type        = "TRANSFER"
event_subtype     = NULL
event_date        = transfer_date
event_date_key    = transfer_date_key
source_fact       = "fact_transfer"
source_record_id  = NULL
source_key        = (player_id, transfer_date, from_club_id, to_club_id)

player_id         = player_id
from_club_id      = from_club_id
to_club_id        = to_club_id
transfer_season   = transfer_season
transfer_fee      = transfer_fee
market_value_in_eur = market_value_in_eur
```

### 12.4. `VALUATION`

Origen:

```text
fact_player_valuation
```

Mapeo principal:

```text
event_type        = "VALUATION"
event_subtype     = NULL
event_date        = valuation_date
event_date_key    = valuation_date_key
source_fact       = "fact_player_valuation"
source_record_id  = NULL
source_key        = (player_id, valuation_date)

player_id         = player_id
club_id           = club_id
competition_id    = competition_id
market_value_in_eur = market_value_in_eur
```

---

## 13. Estrategia de construcción con Spark

La construcción se realiza en dos etapas.

### 13.1. Proyección de cada fact

Cada fact se transforma al esquema canónico de la OBT:

```text
fact_player_match      -> PLAYER_MATCH
fact_match_event       -> MATCH_ACTION
fact_transfer          -> TRANSFER
fact_player_valuation  -> VALUATION
```

Cada proyección:

1. crea `event_type`;
2. construye `source_key`;
3. genera `event_id`;
4. renombra su fecha a `event_date`;
5. proyecta sus campos propios al esquema OBT;
6. añade como `NULL` las columnas que no aplican.

### 13.2. Unión y enriquecimiento

Las cuatro proyecciones se combinan mediante una unión vertical compatible por nombre.

Conceptualmente:

```text
PLAYER_MATCH ──────┐
MATCH_ACTION ──────┤
TRANSFER ──────────┼──> UNION BY NAME ──> enrich dimensions ──> obt_football_events
VALUATION ─────────┘
```

Después de la unión se realizan únicamente joins many-to-one con:

```text
dim_date
dim_player
dim_club             -- con varios roles
dim_competition
```

Los joins con `dim_club`, `dim_competition` y los jugadores secundarios son `LEFT JOIN`.

El join con `dim_date` es obligatorio.

---

## 14. Cardinalidad y validaciones

### 14.1. Conservación por tipo de evento

Debe cumplirse:

```text
COUNT(OBT WHERE event_type = 'PLAYER_MATCH')
=
COUNT(fact_player_match)

COUNT(OBT WHERE event_type = 'MATCH_ACTION')
=
COUNT(fact_match_event)

COUNT(OBT WHERE event_type = 'TRANSFER')
=
COUNT(fact_transfer)

COUNT(OBT WHERE event_type = 'VALUATION')
=
COUNT(fact_player_valuation)
```

### 14.2. Conservación total

```text
N_OBT
=
N_fact_player_match
+ N_fact_match_event
+ N_fact_transfer
+ N_fact_player_valuation
```

La OBT no debe aplicar filtros adicionales que reduzcan este número.

### 14.3. Unicidad global

```sql
COUNT(*) = COUNT(DISTINCT event_id)
```

### 14.4. Cardinalidad de joins

Para cada enriquecimiento:

```text
filas antes del join = filas después del join
```

Si un join aumenta las filas, la dimensión o la condición de unión no satisface la relación many-to-one esperada.

### 14.5. Dominio de `event_type`

```text
PLAYER_MATCH
MATCH_ACTION
TRANSFER
VALUATION
```

No se admiten otros valores.

### 14.6. Consistencia mínima por tipo

```text
PLAYER_MATCH
    game_id IS NOT NULL
    player_id IS NOT NULL

MATCH_ACTION
    game_id IS NOT NULL
    event_subtype IS NOT NULL
    minute IS NOT NULL

TRANSFER
    player_id IS NOT NULL
    transfer_season IS NOT NULL
    (from_club_id IS NOT NULL OR to_club_id IS NOT NULL)

VALUATION
    player_id IS NOT NULL
    market_value_in_eur IS NOT NULL
```

Estas condiciones no constituyen un nuevo filtro. Deben cumplirse porque ya forman parte del contrato de Gold. Si no se cumplen, la construcción debe fallar.

---

## 15. Relación con el futuro modelo

La OBT conserva una línea temporal unificada:

```text
player_id = X

2024-01-15 | VALUATION
2024-01-21 | PLAYER_MATCH
2024-01-21 | MATCH_ACTION
2024-01-28 | PLAYER_MATCH
2024-06-30 | TRANSFER
2024-07-10 | VALUATION
```

A partir de esta tabla puede construirse posteriormente un dataset de entrenamiento con grain:

```text
(player_id, observation_date)
```

La OBT no calcula todavía:

```text
value_change_3m
value_change_12m
depreciation_pct_12m
target_depreciation
minutes_last_90d
matches_last_180d
goals_last_365d
months_remaining_on_contract
```

Estas variables requieren ventanas o una fecha de observación explícita.

`months_remaining_on_contract` se derivará de:

```text
contract_expiration_date
-
observation_date
```

únicamente cuando la información contractual disponible sea válida para esa fecha de observación. El contrato del snapshot actual no debe proyectarse retrospectivamente sobre observaciones históricas sin evidencia point-in-time.

---

## 16. Reacción ante incumplimientos

| Situación | Acción en la OBT |
|---|---|
| Fila válida presente en una fact Gold | Incluir exactamente una fila en la OBT. |
| Campo que no aplica al `event_type` | Mantener `NULL`. |
| Dimensión opcional no encontrada | Conservar la fila; atributos descriptivos del join quedan `NULL`. |
| `player_id` es `NULL` en `MATCH_ACTION` | Conservar la fila. |
| Falta un campo común obligatorio (`event_id`, `event_type`, `event_date`, `event_date_key`, `source_fact`, `source_key`) | **Fallar la construcción.** |
| No existe la fila correspondiente en `dim_date` | **Fallar la construcción.** |
| `event_id` aparece duplicado | **Fallar la construcción.** |
| Aparece un `event_type` fuera del dominio definido | **Fallar la construcción.** |
| Un join aumenta el número de observaciones | **Fallar la construcción.** |
| El conteo de un `event_type` no coincide con su fact Gold | **Fallar la validación.** |
| `N_OBT` no coincide con la suma de las cuatro facts Gold | **Fallar la validación.** |
| `contract_expiration_date` es `NULL` | Conservar la fila; no se podrá derivar información contractual para esa observación. |

---

## 17. Contrato final de salida

La salida final es una única tabla:

```text
obt_football_events
```

con cuatro familias de observaciones:

```text
PLAYER_MATCH
MATCH_ACTION
TRANSFER
VALUATION
```

El contrato fundamental es:

```text
una fila Gold
    ->
exactamente una fila OBT

sin multiplicaciones
sin deduplicación arbitraria
sin pérdida de observaciones
```

## Persistencia e identidad determinista

### Destino de la OBT

La salida de Spark se persiste en Snowflake como:

```text
GOLD.OBT_FOOTBALL_EVENTS
```

`GOLD` es el schema físico de destino. Esto no modifica la separación lógica
del pipeline: las dimensiones y facts Gold son construidas por dbt y
`OBT_FOOTBALL_EVENTS` se construye posteriormente con Spark a partir de ellas.

La escritura utiliza semántica de **reemplazo completo (`overwrite`)**.

```text
Gold actual
    ↓
Spark reconstruye la OBT completa
    ↓
overwrite GOLD.OBT_FOOTBALL_EVENTS
```

No se utiliza `append`.

Por tanto, ejecutar nuevamente el pipeline con el mismo estado de Gold debe
producir el mismo contenido lógico de `GOLD.OBT_FOOTBALL_EVENTS`, sin acumular
filas de ejecuciones anteriores.

La OBT mantiene así la misma propiedad de idempotencia utilizada en las capas
anteriores del pipeline.

---

### Serialización canónica de `source_key`

`source_key` es la representación legible y determinista de la clave natural
de la observación Gold.

Se utilizan las siguientes reglas:

1. Los componentes aparecen siempre en un orden fijo definido por el grain.
2. Los enteros se representan en base 10, sin espacios ni formato adicional.
3. Las fechas se representan en formato ISO `YYYY-MM-DD`.
4. Un valor `NULL` se representa exactamente mediante el literal `<NULL>`.
5. Los componentes se separan mediante `|`.
6. Cada componente incluye el nombre del campo mediante `campo=valor`.
7. No se añaden espacios.
8. La misma fila Gold debe producir exactamente el mismo `source_key` en todas
   las ejecuciones.

Las representaciones son:

```text
PLAYER_MATCH
game_id=<game_id>|player_id=<player_id>

MATCH_ACTION
game_event_id=<game_event_id>

TRANSFER
player_id=<player_id>|transfer_date=<YYYY-MM-DD>|from_club_id=<value-or-<NULL>>|to_club_id=<value-or-<NULL>>

VALUATION
player_id=<player_id>|valuation_date=<YYYY-MM-DD>
```

Ejemplos:

```text
game_id=4312456|player_id=12345

game_event_id=abc123

player_id=12345|transfer_date=2025-07-01|from_club_id=10|to_club_id=25

player_id=12345|transfer_date=2025-07-01|from_club_id=<NULL>|to_club_id=25

player_id=12345|valuation_date=2025-06-30
```

Los campos utilizados actualmente en las claves naturales son identificadores
numéricos, fechas o identificadores fuente controlados, por lo que esta
representación es suficiente y no depende de atributos descriptivos.

---

### Construcción canónica de `event_id`

`event_id` es un identificador técnico global de 64 caracteres hexadecimales.

Primero se construye el payload:

```text
<event_type>|<source_key>
```

y posteriormente:

```text
event_id = SHA256_UTF8(payload)
```

donde SHA-256 se calcula sobre los bytes UTF-8 exactos de la cadena, sin
espacios adicionales, saltos de línea ni modificaciones de casing.

Por tipo:

```text
PLAYER_MATCH
SHA256(
    "PLAYER_MATCH|game_id=<game_id>|player_id=<player_id>"
)

MATCH_ACTION
SHA256(
    "MATCH_ACTION|game_event_id=<game_event_id>"
)

TRANSFER
SHA256(
    "TRANSFER|player_id=<player_id>|transfer_date=<YYYY-MM-DD>|from_club_id=<value-or-<NULL>>|to_club_id=<value-or-<NULL>>"
)

VALUATION
SHA256(
    "VALUATION|player_id=<player_id>|valuation_date=<YYYY-MM-DD>"
)
```

El `event_type` forma parte del payload para separar explícitamente los dominios
de identidad de las cuatro familias de observaciones.

Debe cumplirse:

```text
misma fila Gold
+ mismo event_type
=
mismo source_key
=
mismo event_id
```

y globalmente:

```sql
COUNT(*) = COUNT(DISTINCT event_id)
```

La construcción de la OBT debe fallar si se detecta una colisión o duplicidad
de `event_id`; nunca debe resolverla mediante deduplicación arbitraria.
```

Esto encaja con el contrato que ya tenéis: `event_id` debía ser SHA-256 determinista basado en `event_type` y el grain original, y `TRANSFER` requería tratamiento explícitamente `NULL`-safe. :chatgpt-content-reference{index="0"}

Además, **no cambiaría `source_record_id`**:

```text
PLAYER_MATCH → appearance_id
MATCH_ACTION → game_event_id
TRANSFER     → NULL
VALUATION    → NULL
```
