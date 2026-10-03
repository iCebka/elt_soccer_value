# Contrato de `silver_player_match`

## Alcance y grain

`silver_player_match` contiene exactamente una fila por `appearance_id`. Parte
de `base_tm__appearances` y añade contexto mediante dos `LEFT JOIN`:

```text
appearances.game_id = silver_games_enriched.game_id

appearances.game_id        = club_games.game_id
appearances.player_club_id = club_games.club_id
```

La captura observada contiene 1.894.350 filas y 1.894.350
`appearance_id`. `silver_games_enriched.game_id` es único y
`base_tm__club_games` tiene 177.916 combinaciones únicas
(`game_id`, `club_id`). Unir `club_games` solo por `game_id` produciría
3.788.700 filas; la clave compuesta conserva 1.894.350.

El modelo, su reconciliación y el fixture comprueban de forma permanente la
conservación de claves/conteos y la ausencia de multiplicación.

## Equipo de la participación y contexto actual

`player_club_id` es el equipo representado en esa participación. Se mantiene
separado de `player_current_club_id`, que es información actual incluida en el
snapshot de appearances. En la captura hay 1.090.673 filas donde son distintos;
por tanto el club actual nunca sustituye al club del partido.

La perspectiva compuesta de `club_games` aporta:

- `matched_player_club_id`, `opponent_club_id` y `player_team_hosting`;
- `player_club_name_at_game` y `opponent_club_name_at_game`, derivados de los
  roles home/away ya contratados por `silver_games_enriched`;
- goles, posiciones y managers del equipo/rival;
- `player_team_result` (`win`, `draw`, `loss` o `unknown`) derivado de los
  goles, conservando también `club_games_is_win` sin reinterpretarlo.

`team_context_status` informa `aligned_home`, `aligned_away`, ausencia de games
o club_games, o una inconsistencia. El snapshot actual presenta cero
inconsistencias de rol, marcador o `is_win`.

Los partidos de selecciones se conservan como cualquier otra participación.
Hay 3.027 appearances en `national_team_competition`: los IDs de participante
siguen siendo los de la fuente, sin inventar una relación con `clubs` o
`national_teams`. Los 14.200 appearances cuya competición no existe en el
catálogo también permanecen.

## Fecha y competición: precedencia explícita

Se mantienen ambos originales:

- `appearance_date_source` y `game_date_source`;
- `appearance_competition_id` y `game_competition_id`.

`match_date` y `competition_id` prefieren `silver_games_enriched`, porque es la
dimensión única del partido. Si games no resuelve o el campo está ausente, usan
appearances como fallback. `match_date_source` y `competition_id_source`
registran la procedencia.

`date_comparison_status` y `competition_comparison_status` distinguen
coincidencia, discrepancia con games preferido, campo ausente y game no
encontrado. Nunca se descarta ni corrige silenciosamente una fila. En la versión
observada los 1.894.350 games resuelven y existen cero discrepancias de fecha o
competición; el fixture prueba explícitamente discrepancias y fallbacks para que
el contrato no dependa de esa cobertura perfecta.

## Estadísticas

El modelo expone `minutes_played`, `goals`, `assists`, `yellow_cards` y
`red_cards` sin rellenar ausencias con cero. Solo se rechazan valores negativos,
siguiendo las reglas base.

No se impone un máximo de minutos. Se observaron 11.041 appearances sobre 90
minutos, tres sobre 120 (máximo 148) y tres con cero minutos. Esos casos se
conservan porque pueden representar prórrogas, errores o convenciones de la
fuente y no existe evidencia suficiente para corregirlos en Silver. Tampoco se
aplican reglas específicas por tipo de competición que eliminen participaciones.

## Estados y trazabilidad

`game_join_status` y `club_game_join_status` hacen visible la cobertura de cada
unión. La captura actual tiene cobertura completa en ambas, pero los `LEFT JOIN`
y el fixture garantizan que una versión futura incompleta conserve las filas.

El modelo guarda versión, checksum, fecha de captura y run Bronze por separado
para `appearances`, `club_games`, `games`, `competitions` y `clubs`, además de
`silver_processed_at`. Cuando la auditoría no proporciona versión/captura se
mantiene `NULL`; no se confunde con la fecha del partido.

## Límite Silver/Gold

Este modelo no une valoraciones. Una unión de rendimiento y valor por
`player_id` solamente sería temporalmente ambigua y multiplicaría hechos. La
combinación as-of entre partidos y valoraciones se implementará posteriormente
en Gold, con una regla temporal explícita.

## Verificación

Los comandos y resultados reales de Stage 5 se registran en
`docs/silver_implementation_progress.md`. No se modifican Bronze ni la ingesta
para probar: los casos sin games/club_games, cambio de club, dos equipos y
discrepancias viven en un fixture SQL aislado.
