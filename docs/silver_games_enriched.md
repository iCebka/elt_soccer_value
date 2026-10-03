# Contrato de `silver_games_enriched`

## Alcance y grain

`silver_games_enriched` es una tabla Silver reproducible con exactamente una
fila por `game_id`. Parte de `base_tm__games` y solo añade catálogos limpios
mediante `ref()` y `LEFT JOIN`:

```text
games.competition_id = competitions.competition_id
games.home_club_id    = home_club.club_id
games.away_club_id    = away_club.club_id
```

Las claves consultadas se verificaron antes de unir: 88.958 filas y 88.958
`game_id`, 65 filas y 65 `competition_id`, y 796 filas y 796 `club_id`. Los
tests permanentes vuelven a comprobar esas tres unicidades, la unicidad final y
la conservación exacta del conjunto y el número de partidos.

## Semántica temporal y roles

Los campos con sufijo `_at_game` proceden directamente de la fila histórica de
`games`: nombres de participantes, tipo de competición, posiciones,
entrenadores y formaciones. El modelo conserva además marcador, resultado
agregado, estadio, asistencia, árbitro y URL del partido.

Los campos de `clubs` llevan el sufijo `_current_snapshot`. Son contexto del
catálogo seleccionado por el manifiesto, no atributos históricos as-of del
partido. Se exponen únicamente nombre, código, competición doméstica, última
temporada y URL. No se publican entrenador actual, tamaño/edad de plantilla ni
valor actual como características del partido.

`competition_type_at_game` conserva el valor de `games` y
`competition_type_snapshot` conserva el del catálogo. No se coalescen: una
diferencia futura debe seguir siendo visible. En la versión observada hay cero
diferencias cuando la competición resuelve. `competition_country_id` es un ID
Transfermarkt, no un código ISO. `competition_country_name` es opcional: está
ausente en 11.236 partidos con competición resuelta, principalmente catálogos
sin país único, y no se inventa a partir de otros campos.

Los nombres de `games` se prefieren para representar el partido. Entre los
participantes que sí resuelven no hay diferencias de nombre normalizado contra
el catálogo actual; 86 partidos carecen de nombre local histórico y 36 de
nombre visitante histórico, por lo que el catálogo no sobrescribe el original.

## Cobertura y estados de resolución

No se eliminan partidos por catálogos incompletos. El perfil observado es:

| Rol | `matched_id` | `not_found` | ID ausente | Total |
|---|---:|---:|---:|---:|
| competición | 87.744 | 1.214 | 0 | 88.958 |
| participante local en `clubs` | 76.140 | 12.818 | 0 | 88.958 |
| participante visitante en `clubs` | 77.716 | 11.242 | 0 | 88.958 |

Los campos se llaman `home_club_id` y `away_club_id` porque así los expone la
fuente, pero en los 742 partidos `national_team_competition` los dos IDs son de
selecciones y ninguno resuelve contra `clubs`. Esos partidos, sus IDs y sus
nombres (por ejemplo, `Germany`) permanecen. La cobertura también es parcial
en copas domésticas e internacionales por límites del snapshot actual de
clubes. No se intenta unir automáticamente esas filas con `national_teams` ni
resolver por nombre.

Cada rol conserva el ID fuente, un `resolved_*_id` nullable y un estado:

- `matched_id`: el ID existe en el catálogo seleccionado;
- `not_found`: hay ID fuente, pero no existe en ese catálogo;
- `missing_source_id`: el ID fuente está ausente. No aparece en el snapshot
  actual, pero el estado está definido para que el contrato detecte futuras
  versiones sin descartar la fila.

Las relaciones de IDs fuente son advertencias debido a la cobertura observada;
las relaciones de IDs resueltos y la consistencia entre ID/estado son errores.

## Fechas, resultados y opcionales

La versión observada abarca de 2006-06-09 a 2026-07-06. `game_date`, ambos IDs
de participante y ambos goles están completos; no hay goles negativos ni
partidos con el mismo ID en ambos roles. Los tests críticos conservan esas
reglas, además de rechazar asistencia negativa o posiciones menores que uno
cuando estén informadas.

No se deriva una regla entre `season` y el año de `game_date`: temporadas y
competiciones pueden cruzar años. Entrenador, posición, formación, asistencia y
nombres son opcionales según cobertura; una ausencia válida no se convierte en
rechazo.

`game_date` es la fecha del hecho. No debe confundirse con
`*_source_captured_at` ni `silver_processed_at`.

## Procedencia reproducible

El modelo conserva por separado para `games`, `competitions` y `clubs`:

- `source_version` y SHA-256;
- fecha real de captura;
- `bronze_ingestion_run_id`.

En el manifiesto verificado los checksums y runs existen, mientras que
`source_version` y `source_captured_at` están `NULL` porque la auditoría Bronze
no los recibió. El modelo conserva esos `NULL` y no fabrica fechas o versiones.
`silver_processed_at` identifica únicamente el procesamiento Silver.

## Contrato para `silver_player_match`

La etapa siguiente podrá enriquecer cada appearance sin multiplicarlo:

```sql
left join {{ ref('silver_games_enriched') }} as games
    on appearances.game_id = games.game_id
```

La relación contractual es
`appearances.game_id = silver_games_enriched.game_id`. El consumidor puede
reutilizar `game_date`, `season`, `game_round`, IDs y nombres local/visitante,
marcador, competición/país, campos `_at_game` y estados de resolución. Debe
mantener su propio grain de una fila por `appearance_id`; los atributos
`*_current_snapshot` continúan siendo contexto actual y no historia del
partido.

## Verificación de esta etapa

Los resultados ejecutados se registran en
`docs/silver_implementation_progress.md`. El fixture aislado cubre roles
local/visitante, un nombre histórico distinto al catálogo, competición/equipos
no encontrados y un partido de selección conservado, sin modificar Bronze.
