# Contrato consolidado de las cuatro tablas Silver

Las cuatro tablas finales usan las versiones Bronze fijadas por
`base_tm__source_manifest`, se materializan como tablas y seleccionan columnas
explícitas. Sus grains son:

| Tabla | Grain contractual | Naturaleza temporal |
|---|---|---|
| `silver_players_current` | una fila por `player_id` | perfil del snapshot actual seleccionado |
| `silver_games_enriched` | una fila por `game_id` | hecho partido y catálogos snapshot separados |
| `silver_player_match` | una fila por `appearance_id` | hecho jugador-partido |
| `silver_player_valuations_enriched` | una fila por (`player_id`, `valuation_date`) | historial de valoraciones con contexto snapshot |

## Relaciones autorizadas

```text
silver_player_match.game_id
    -> silver_games_enriched.game_id

silver_player_valuations_enriched.player_id
    -> silver_players_current.player_id

players.current_national_team_id
    -> national_teams.national_team_id
    -> countries.country_id
```

`silver_player_match` resuelve la perspectiva del equipo con
(`game_id`, `player_club_id`) contra (`club_games.game_id`,
`club_games.club_id`). No se permite unir `club_games` solo por `game_id`.

No existe una relación directa de valoraciones con selecciones. Tampoco se
unen valoraciones y partidos solo por `player_id`: ese cruce multiplicaría
hechos y carecería de una regla temporal.

## Semántica temporal común

- `_at_game` describe campos almacenados en la fila de games/club_games.
- `_current_snapshot` o `_snapshot` identifica atributos del snapshot actual,
  aunque se repliquen sobre hechos históricos.
- `valuation_date`, `game_date` y `match_date` son fechas de negocio; nunca se
  sustituyen por fechas de captura o procesamiento.
- cada tabla conserva checksums, runs y fechas de captura de sus fuentes;
  valores no proporcionados permanecen `NULL`.
- IDs de país son identificadores Transfermarkt, no se asumen códigos ISO.

Los `LEFT JOIN` conservan hechos aunque falten perfiles o catálogos. Los campos
`*_resolution_status`, `*_join_status` y de comparación hacen visible la
cobertura sin inventar correspondencias.

## Límite con Gold

Silver entrega entidades limpias y trazables. Quedan fuera de estas cuatro
tablas:

- la etiqueta predictiva;
- ventanas y agregaciones de rendimiento;
- joins as-of entre partidos y valoraciones;
- reconstrucción histórica de atributos que solo existen en snapshots;
- features y el modelo predictivo.

Los contratos detallados viven en `docs/silver_players_current.md`,
`docs/silver_games_enriched.md`, `docs/silver_player_match.md` y
`docs/silver_player_valuations_enriched.md`.
