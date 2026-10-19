# Gold — Decisiones del modelo dimensional

## 1. Principios generales

La capa Gold reorganiza Silver en un modelo dimensional orientado al análisis y a la construcción posterior de una OBT de eventos.

Se aplican las siguientes reglas generales:

- Gold hereda de Silver los tipos y las normalizaciones ya aplicadas.
- Gold no vuelve a limpiar valores ni reinterpreta *sentinels* definidos en Silver.
- Cada tabla Gold tiene un **grain explícito**.
- Una fila entra a Gold únicamente si satisface el **criterio de inclusión** de la tabla correspondiente.
- Un `NULL` en un campo opcional no elimina por sí solo una fila.
- Los *joins* de enriquecimiento no pueden aumentar el número de observaciones.
- No se usa `DISTINCT` para ocultar multiplicaciones producidas por *joins*.
- Si se viola la unicidad del grain, el modelo o su test debe fallar.
- No se exige integridad referencial estricta hacia dimensiones cuya cobertura histórica sea incompleta, especialmente `dim_club` y `dim_competition`.
- Gold termina en dimensiones y tablas de hechos. Las ventanas temporales, las variables del modelo y la OBT se construyen posteriormente con Spark.

---

## 2. Dimensiones

### 2.1. `dim_player`

**Grain:** un jugador (`player_id`).

**Fuente principal:** `silver.players`.

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `player_id` | `INTEGER` | `NOT NULL`; `UNIQUE` | Clave de la dimensión. |
| `player_code` | `VARCHAR` | Nullable | Código descriptivo normalizado en Silver. |
| `name` | `VARCHAR` | `NOT NULL` | Nombre principal. |
| `first_name` | `VARCHAR` | Nullable | Atributo descriptivo. |
| `last_name` | `VARCHAR` | Nullable | Atributo descriptivo. |
| `date_of_birth` | `DATE` | `NOT NULL` | Necesario para calcular la edad en observaciones históricas. |
| `country_of_birth` | `VARCHAR` | Nullable | Información demográfica. |
| `city_of_birth` | `VARCHAR` | Nullable | Información demográfica. |
| `country_of_citizenship` | `VARCHAR` | Nullable | Información demográfica. |
| `position` | `VARCHAR` | Nullable | Característica deportiva relativamente estable. |
| `sub_position` | `VARCHAR` | Nullable | Característica deportiva relativamente estable. |
| `foot` | `VARCHAR` | Nullable | Dominio normalizado en Silver. |
| `height_in_cm` | `INTEGER` | Nullable | Medida descriptiva. |
| `contract_expiration_date` | `DATE` | Nullable | Estado contractual disponible en el snapshot. No debe proyectarse retrospectivamente. |

#### Criterio de inclusión

```sql
player_id IS NOT NULL
AND name IS NOT NULL
AND date_of_birth IS NOT NULL
```

Si no se cumple este criterio, la fila no entra a `dim_player`.

#### Campos de Silver que no se proyectan a la dimensión principal

```text
current_club_id
current_club_name
current_club_domestic_competition_id
current_national_team_id
market_value_in_eur
highest_market_value_in_eur
```

Estos campos representan principalmente el estado actual y no deben proyectarse retrospectivamente sobre hechos históricos.

`contract_expiration_date` sí se conserva como atributo nullable porque es relevante para el caso de uso final. Sin embargo, corresponde al estado contractual disponible en el snapshot y no debe tratarse como si fuera históricamente válido para fechas anteriores.

---

### 2.2. `dim_club`

**Grain:** un club (`club_id`).

**Fuente principal:** `silver.clubs`.

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `club_id` | `INTEGER` | `NOT NULL`; `UNIQUE` | Clave de la dimensión. |
| `club_code` | `VARCHAR` | Nullable | Código normalizado. |
| `name` | `VARCHAR` | `NOT NULL` | Nombre principal. |
| `domestic_competition_id` | `VARCHAR` | Nullable | Competición doméstica del snapshot actual. |
| `stadium_name` | `VARCHAR` | Nullable | Contexto descriptivo. |
| `stadium_seats` | `INTEGER` | Nullable | Contexto descriptivo. |
| `url` | `VARCHAR` | Nullable | Referencia a la entidad fuente. |

#### Criterio de inclusión

```sql
club_id IS NOT NULL
AND name IS NOT NULL
```

Si no se cumple este criterio, la fila no entra a `dim_club`.

No se exige que todos los `club_id` presentes en hechos históricos existan en `dim_club`.

#### Campos de Silver que no se proyectan a la dimensión principal

```text
total_market_value
squad_size
average_age
foreigners_number
foreigners_percentage
national_team_players
net_transfer_record
coach_name
```

Estos son atributos de snapshot potencialmente variables en el tiempo.

---

### 2.3. `dim_competition`

**Grain:** una competición (`competition_id`).

**Fuente principal:** `silver.competitions`.

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `competition_id` | `VARCHAR` | `NOT NULL`; `UNIQUE` | Clave de la dimensión. |
| `competition_code` | `VARCHAR` | Nullable | Código normalizado. |
| `name` | `VARCHAR` | `NOT NULL` | Nombre principal. |
| `sub_type` | `VARCHAR` | Nullable | Clasificación de la competición. |
| `type` | `VARCHAR` | Nullable | Clasificación de la competición. |
| `country_id` | `INTEGER` | Nullable | Mantiene la convención definida en Silver. |
| `country_name` | `VARCHAR` | Nullable | No aplica necesariamente a competiciones internacionales. |
| `domestic_league_code` | `VARCHAR` | Nullable | No aplica a todas las competiciones. |
| `confederation` | `VARCHAR` | Nullable | Contexto regional. |
| `url` | `VARCHAR` | Nullable | Referencia a la entidad fuente. |

#### Criterio de inclusión

```sql
competition_id IS NOT NULL
AND name IS NOT NULL
```

Si no se cumple este criterio, la fila no entra a `dim_competition`.

`country_name` y `confederation` permanecen en esta dimensión para evitar un modelo *snowflake* innecesario.

---

### 2.4. `dim_date`

**Grain:** un día calendario (`date_key`).

**Fuente:** dimensión generada.

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `date_key` | `INTEGER` | `NOT NULL`; `UNIQUE`; formato `YYYYMMDD` | Clave temporal. |
| `date` | `DATE` | `NOT NULL`; `UNIQUE` | Fecha calendario. |
| `year` | `INTEGER` | `NOT NULL` | Año. |
| `quarter` | `INTEGER` | `NOT NULL`; entre `1` y `4` | Trimestre. |
| `month` | `INTEGER` | `NOT NULL`; entre `1` y `12` | Mes. |
| `month_name` | `VARCHAR` | `NOT NULL` | Nombre del mes. |
| `day` | `INTEGER` | `NOT NULL` | Día del mes. |
| `day_of_week` | `INTEGER` | `NOT NULL` | Día de la semana. |
| `day_name` | `VARCHAR` | `NOT NULL` | Nombre del día. |
| `week_of_year` | `INTEGER` | `NOT NULL` | Semana calendario. |

#### Criterio de generación

Se genera un calendario continuo entre la fecha mínima y la fecha máxima relevantes para las tablas de hechos Gold.

La dimensión generada debe cumplir:

```sql
COUNT(*) = COUNT(DISTINCT date_key)
COUNT(*) = COUNT(DISTINCT date)
```

Si alguna de estas condiciones no se cumple, el modelo falla.

---

## 3. Tablas de hechos

### 3.1. `fact_player_valuation`

**Grain:** una valoración de mercado de un jugador en una fecha.

**Fuente principal:** `silver.player_valuations`.

**Clave natural:**

```text
(player_id, valuation_date)
```

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `player_id` | `INTEGER` | `NOT NULL`; debe existir en `dim_player` | Jugador valorado. |
| `valuation_date` | `DATE` | `NOT NULL` | Fecha de la observación. |
| `valuation_date_key` | `INTEGER` | `NOT NULL`; FK lógica a `dim_date` | Formato `YYYYMMDD`. |
| `club_id` | `INTEGER` | Nullable | Puede representar ausencia de club; no se exige FK estricta a `dim_club`. |
| `competition_id` | `VARCHAR` | Nullable | Puede no aplicar si no existe club. |
| `market_value_in_eur` | `DECIMAL` | `NOT NULL`; `>= 0` | Medida principal. |

#### Criterio de inclusión

```sql
player_id IS NOT NULL
AND valuation_date IS NOT NULL
AND market_value_in_eur IS NOT NULL
AND player_id EXISTS IN dim_player
```

Si no se cumple este criterio, la fila se descarta de `fact_player_valuation`.

#### Unicidad

```text
UNIQUE(player_id, valuation_date)
```

Si se viola esta restricción, el modelo o su test falla.

En Gold todavía no se calculan `value_change`, la depreciación futura ni el *target* a doce meses.

---

### 3.2. `fact_player_match`

**Grain:** la participación de un jugador en un partido.

**Fuentes:** `silver.appearances` como base, enriquecida con `silver.games`, `silver.club_games` y una proyección de `silver.game_lineups` reducida a una fila por `(game_id, player_id)`.

**Clave natural:**

```text
(game_id, player_id)
```

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `appearance_id` | `VARCHAR` | `NOT NULL`; `UNIQUE` | Identificador fuente y de *lineage*. |
| `game_id` | `INTEGER` | `NOT NULL` | Parte del grain. |
| `player_id` | `INTEGER` | `NOT NULL`; debe existir en `dim_player` | Parte del grain. |
| `game_date` | `DATE` | `NOT NULL` | Se obtiene de `games`. |
| `game_date_key` | `INTEGER` | `NOT NULL`; FK lógica a `dim_date` | Formato `YYYYMMDD`. |
| `club_id` | `INTEGER` | Nullable | Club representado por el jugador. |
| `opponent_club_id` | `INTEGER` | Nullable | Derivado de `club_games`. |
| `competition_id` | `VARCHAR` | Nullable | Contexto del partido; no se exige FK estricta. |
| `home_or_away` | `VARCHAR` | Nullable; `{home, away}` cuando exista | Contexto del partido. |
| `is_win` | `BOOLEAN` | Nullable | Resultado desde la perspectiva del club. |
| `is_starter` | `BOOLEAN` | Nullable | Derivado de `game_lineups`. |
| `is_captain` | `BOOLEAN` | Nullable | Derivado de `game_lineups`. |
| `minutes_played` | `INTEGER` | Nullable; `>= 0` | Medida. |
| `goals` | `INTEGER` | Nullable; `>= 0` | Medida. |
| `assists` | `INTEGER` | Nullable; `>= 0` | Medida. |
| `yellow_cards` | `INTEGER` | Nullable; `>= 0` | Medida disciplinaria. |
| `red_cards` | `INTEGER` | Nullable; `>= 0` | Medida disciplinaria. |

#### Criterio de inclusión

```sql
game_id IS NOT NULL
AND player_id IS NOT NULL
AND game_id EXISTS IN silver.games
AND games.date IS NOT NULL
AND player_id EXISTS IN dim_player
```

Si no se cumple este criterio, la fila se descarta de `fact_player_match`.

No se exige que `club_id`, `opponent_club_id` o `competition_id` existan en sus dimensiones correspondientes.

#### Unicidad

```text
UNIQUE(game_id, player_id)
```

Si se viola esta restricción, el modelo o su test falla.

El *join* con `game_lineups` solo se realiza después de obtener una proyección única por `(game_id, player_id)`.

---

### 3.3. `fact_match_event`

**Grain:** una acción registrada dentro de un partido.

**Fuentes:** `silver.game_events` como base, enriquecida con `silver.games`.

**Clave natural:**

```text
game_event_id
```

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `game_event_id` | `VARCHAR` | `NOT NULL`; `UNIQUE` | Identificador natural. |
| `game_id` | `INTEGER` | `NOT NULL` | Partido donde ocurrió la acción. |
| `event_date` | `DATE` | `NOT NULL` | Fecha de la acción. |
| `event_date_key` | `INTEGER` | `NOT NULL`; FK lógica a `dim_date` | Formato `YYYYMMDD`. |
| `competition_id` | `VARCHAR` | Nullable | Derivado del partido. |
| `minute` | `INTEGER` | `NOT NULL`; `-1` se conserva; los demás valores deben ser `>= 0` | Convención definida en Silver. |
| `event_type` | `VARCHAR` | `NOT NULL` | Tipo de acción normalizado. |
| `player_id` | `INTEGER` | Nullable | Algunos eventos legítimos no tienen jugador principal. |
| `club_id` | `INTEGER` | Nullable | Depende del evento. |
| `player_assist_id` | `INTEGER` | Nullable | Solo aplica a ciertos eventos. |
| `player_in_id` | `INTEGER` | Nullable | Solo aplica a sustituciones. |
| `description` | `VARCHAR` | Nullable | Descripción de la fuente. |

#### Criterio de inclusión

```sql
game_event_id IS NOT NULL
AND game_id IS NOT NULL
AND event_date IS NOT NULL
AND event_type IS NOT NULL
AND game_id EXISTS IN silver.games
```

Si no se cumple este criterio, la fila se descarta de `fact_match_event`.

`player_id IS NULL` no elimina el evento porque el jugador no forma parte del grain.

#### Unicidad

```text
UNIQUE(game_event_id)
```

Si se viola esta restricción, el modelo o su test falla.

---

### 3.4. `fact_transfer`

**Grain:** un movimiento de transferencia de un jugador.

**Fuente principal:** `silver.transfers`.

**Clave natural:**

```text
(player_id, transfer_date, from_club_id, to_club_id)
```

| Campo | Tipo Gold | Regla Gold | Nota |
|---|---|---|---|
| `player_id` | `INTEGER` | `NOT NULL`; debe existir en `dim_player` | Jugador transferido. |
| `transfer_date` | `DATE` | `NOT NULL` | Fecha del movimiento. |
| `transfer_date_key` | `INTEGER` | `NOT NULL`; FK lógica a `dim_date` | Formato `YYYYMMDD`. |
| `transfer_season` | `VARCHAR` | `NOT NULL` | Texto con formato tipo `25/26`. |
| `from_club_id` | `INTEGER` | Nullable | Puede representar un origen sin club. |
| `to_club_id` | `INTEGER` | Nullable | Puede representar un destino sin club. |
| `transfer_fee` | `DECIMAL` | Nullable; `>= 0` cuando exista | `NULL` no significa cero. |
| `market_value_in_eur` | `DECIMAL` | Nullable; `>= 0` cuando exista | Valor registrado en el movimiento. |

#### Criterio de inclusión

```sql
player_id IS NOT NULL
AND transfer_date IS NOT NULL
AND transfer_season IS NOT NULL
AND (from_club_id IS NOT NULL OR to_club_id IS NOT NULL)
AND player_id EXISTS IN dim_player
```

Si no se cumple este criterio, la fila se descarta de `fact_transfer`.

No se exige que `from_club_id` o `to_club_id` existan en `dim_club`.

#### Unicidad

```text
UNIQUE(player_id, transfer_date, from_club_id, to_club_id)
```

La validación debe tratar los valores `NULL` de manera consistente, de modo que dos claves equivalentes con un lado `NULL` no se consideren diferentes.

Si se viola esta restricción, el modelo o su test falla.

---

## 4. Relaciones principales

| Fact | Dimensión | Relación | Obligatoria para incluir la fila |
|---|---|---|---|
| `fact_player_valuation` | `dim_player` | `player_id` | Sí |
| `fact_player_valuation` | `dim_date` | `valuation_date_key` | Sí |
| `fact_player_valuation` | `dim_club` | `club_id` | No |
| `fact_player_valuation` | `dim_competition` | `competition_id` | No |
| `fact_player_match` | `dim_player` | `player_id` | Sí |
| `fact_player_match` | `dim_date` | `game_date_key` | Sí |
| `fact_player_match` | `dim_club` | `club_id`, `opponent_club_id` | No |
| `fact_player_match` | `dim_competition` | `competition_id` | No |
| `fact_match_event` | `dim_date` | `event_date_key` | Sí |
| `fact_match_event` | `dim_player` | `player_id` | No |
| `fact_match_event` | `dim_club` | `club_id` | No |
| `fact_match_event` | `dim_competition` | `competition_id` | No |
| `fact_transfer` | `dim_player` | `player_id` | Sí |
| `fact_transfer` | `dim_date` | `transfer_date_key` | Sí |
| `fact_transfer` | `dim_club` | `from_club_id`, `to_club_id` | No |

---

## 5. Compatibilidad con la OBT final

Spark consumirá las cuatro tablas de hechos y las proyectará a una interfaz temporal común.

| `event_type` en la OBT | Fact Gold de origen | Grain original |
|---|---|---|
| `PLAYER_MATCH` | `fact_player_match` | Jugador × partido |
| `MATCH_ACTION` | `fact_match_event` | Acción registrada en un partido |
| `TRANSFER` | `fact_transfer` | Transferencia |
| `VALUATION` | `fact_player_valuation` | Jugador × fecha de valoración |

La OBT tendrá como grain una **observación temporal del dominio**. Los cuatro grains de Gold se conservan y se identifican mediante `event_type`.

Gold todavía no calcula:

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

Estas variables pertenecen a Spark y al proceso posterior de *feature engineering*.

`months_remaining_on_contract` se calculará a partir de `dim_player.contract_expiration_date` y la fecha de observación. Para el entrenamiento histórico, solo debe utilizarse cuando la fecha contractual sea válida para ese momento; el valor contractual del snapshot actual no debe proyectarse retrospectivamente.

---

## 6. Política de cardinalidad

Para cada tabla de hechos debe cumplirse:

```text
N_gold = N_silver_elegible
```

donde `N_silver_elegible` representa las filas que satisfacen el criterio de inclusión de la tabla Gold.

Para cualquier *join* de enriquecimiento debe cumplirse:

```text
filas antes del join = filas después del join
```

Las dimensiones deben satisfacer:

```sql
COUNT(*) = COUNT(DISTINCT dimension_key)
```

Las tablas de hechos deben satisfacer las siguientes restricciones de unicidad:

```text
fact_player_valuation
    UNIQUE(player_id, valuation_date)

fact_player_match
    UNIQUE(game_id, player_id)

fact_match_event
    UNIQUE(game_event_id)

fact_transfer
    UNIQUE(player_id, transfer_date, from_club_id, to_club_id)
```

---

## 7. Reacción ante incumplimientos

| Situación | Acción en Gold |
|---|---|
| Una fila no cumple el criterio de inclusión de la dimensión o fact | **Descartar la fila de esa tabla Gold.** |
| Un campo opcional es `NULL` | Conservar la fila. |
| No existe una relación obligatoria con una dimensión | **Descartar la fila.** |
| No existe una relación no obligatoria con una dimensión | Conservar la fila y el identificador disponible. |
| El grain o una clave declarada `UNIQUE` aparece duplicada | **Descartar todas las filas involucradas en la duplicidad.** |
| Un *join* aumenta el número de observaciones | **Fallar el modelo o el test.** |
| Un *join* no encuentra una dimensión opcional | Conservar la observación. |
| Una fact conserva el mismo número de filas que Silver total, pero incluye filas no elegibles | **Fallar la validación.** |
| Una fact pierde filas que sí eran elegibles | **Fallar la validación.** |

---

## 8. Alcance final de Gold

Gold queda compuesto por las siguientes ocho tablas:

### Dimensiones

```text
dim_player
dim_club
dim_competition
dim_date
```

### Tablas de hechos

```text
fact_player_valuation
fact_player_match
fact_match_event
fact_transfer
```

Estas ocho tablas constituyen el contrato de entrada para la construcción posterior de la OBT con Spark.
