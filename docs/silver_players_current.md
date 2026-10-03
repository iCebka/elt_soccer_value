# Contrato de `silver_players_current`

## Alcance y grain

`silver_players_current` es una tabla Silver reproducible con exactamente una
fila por `player_id`. Se construye solo con `ref()` sobre
`base_tm__players`, `base_tm__national_teams`, `base_tm__countries`,
`base_tm__clubs`, el lookup intermedio de país y el manifiesto de fuentes.

Todos los enriquecimientos son `LEFT JOIN`. Un jugador permanece aunque no
tenga club o selección, aunque el ID no exista en el snapshot dimensional, o
aunque nacimiento/ciudadanía no puedan resolverse.

El snapshot verificado tiene 50.149 filas y 50.149 jugadores distintos, igual
que `base_tm__players`.

## Relaciones por ID

Las uniones son exclusivamente las observadas en la fuente:

```text
players.current_national_team_id = national_teams.national_team_id
national_teams.country_id        = countries.country_id
players.current_club_id          = clubs.club_id
```

`current_national_team_id` existe en el CSV `players`, staging y la entidad
base. No se deduce la selección desde ciudadanía, nacimiento o nombres.

Cobertura actual:

| Rol | Estado | Jugadores |
|---|---|---:|
| selección actual | `matched_id` | 3.154 |
| selección actual | `missing_source_id` | 46.889 |
| selección actual | `not_found` | 106 |
| país de selección | `matched_id` | 3.154 |
| país de selección | `not_applicable` | 46.889 |
| país de selección | `national_team_not_found` | 106 |
| club actual | `matched_id` | 47.163 |
| club actual | `not_found` | 2.986 |

Los 106 IDs de selección y 2.986 IDs de club sin dimensión permanecen como
IDs fuente; sus columnas `resolved_*` son `NULL`. Los tests sobre IDs fuente son
advertencias y las relaciones de IDs resueltos son errores.

## Nacimiento y ciudadanía

Son roles independientes. El modelo retiene:

- `country_of_birth_source`, `birth_country_id`, `birth_country_name` y
  `birth_country_resolution_status`;
- `country_of_citizenship_source`, `citizenship_country_id`,
  `citizenship_country_name` y `citizenship_country_resolution_status`;
- `national_team_country_*`, sin mezclarlo con los dos anteriores.

Los `country_id` son identificadores Transfermarkt y no se interpretan como
códigos ISO. Los nombres se comparan solo después de colapsar espacios,
recortar extremos y convertir a mayúsculas. No hay `LIKE`, distancia de
edición, fonética ni otro matching difuso.

El seed `tm_country_name_aliases.csv` contiene solo dos variantes observadas
que resuelven a una fila del catálogo:

| Variante observada | Nombre canónico ingerido | Justificación |
|---|---|---|
| `Turkey` | `Türkiye` | nombre inglés anterior del país renombrado |
| `Macedonia` | `North Macedonia` | nombre corto anterior del país renombrado |

Nombres históricos como `UdSSR`, `Jugoslawien (SFR)`, `CSSR` o `East Germany
(GDR)` no se fuerzan a países actuales. `United Kingdom` queda sin resolver
porque el catálogo separa England, Scotland, Wales y Northern Ireland. Países
no presentes en `countries`, como `Cote d'Ivoire`, también quedan `not_found`.

Cada clave normalizada y cada alias debe resolver como máximo una fila. Los
tests fallan si aparece una duplicidad o si un alias no tiene exactamente un
destino. El fixture de ambigüedad confirma que el resultado queda `NULL` con
estado `ambiguous`, sin escoger el menor ID ni otro ganador arbitrario.

### Reporte observado

| Rol | `matched_name` | `matched_alias` | `missing` | `not_found` | `ambiguous` |
|---|---:|---:|---:|---:|---:|
| nacimiento | 38.994 | 1.242 | 5.885 | 4.028 | 0 |
| ciudadanía | 45.980 | 1.392 | 269 | 2.508 | 0 |

Hay 3.372 jugadores cuyo nacimiento y ciudadanía resueltos son distintos.

## Ciudadanías múltiples

La versión ingerida expone `country_of_citizenship` como un campo escalar. No
se observaron `/`, `;` ni `|`; las únicas comas corresponden a los nombres
escalares `Korea, South` y `Korea, North`. Por eso no se crea
`silver_player_citizenships` en esta etapa.

`assert_player_citizenship_is_scalar` convierte esta observación en contrato:
si una versión futura presenta un separador múltiple, el build falla y deberá
implementarse el puente antes de aceptar esos datos. Nunca se elegirá una
ciudadanía arbitraria ni se expandirá el grain principal.

## Contexto actual y procedencia

Los atributos de club, selección, valor, contrato, participaciones/goles
internacionales acumulados y ranking FIFA describen los snapshots seleccionados
por el manifiesto. No representan automáticamente el estado del jugador en una
fecha histórica.

El modelo conserva por separado para `players`, `national_teams`, `countries` y
`clubs`:

- `source_version` y SHA-256;
- fecha real de captura;
- `bronze_ingestion_run_id`.

`silver_processed_at` es el procesamiento Silver y no reemplaza ninguna fecha
de captura o negocio.

## Contrato para el modelo de valoraciones

El modelo de valoraciones podrá usar:

```sql
left join {{ ref('silver_players_current') }} as players
    on valuations.player_id = players.player_id
```

La unicidad de `player_id` garantiza que esa unión no multiplica el historial
de valoraciones. Debe conservar `valuation_date` como fecha del hecho y etiquetar
los atributos de este modelo como contexto del snapshot actual; no debe tratarlos
como valores as-of de cada fecha histórica.

## Verificación ejecutada

- build con manifiesto fijo: 69 nodos, 63 PASS, 6 WARN de cobertura, 0 ERROR;
- repetición del modelo y 23 tests: 22 PASS, 2 WARN, 0 ERROR;
- cardinalidad antes/después: 50.149 / 50.149;
- huella de negocio antes/después: idéntica;
- fixtures: jugador sin selección, roles de país distintos, país ausente,
  nombre normalizado ambiguo y alias ambiguo.

Las verificaciones se ejecutaron en `DBT_TEST_SILVER`; no se escribió en el
schema productivo.
