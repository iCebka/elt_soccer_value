# Contrato de `silver_player_valuations_enriched`

## Alcance y grain

`silver_player_valuations_enriched` conserva todo el historial aceptado de
`base_tm__player_valuations`: exactamente una fila por
(`player_id`, `valuation_date`). No selecciona la última valoración ni reduce a
una fila por jugador.

La captura observada contiene 656.301 valoraciones y 656.301 claves candidatas
únicas para 41.528 jugadores. De ellos, 40.465 tienen varias valoraciones y el
máximo observado es 57. Las fechas abarcan de 2000-01-20 a 2026-06-12.

El único enriquecimiento de entidad es:

```text
player_valuations.player_id = silver_players_current.player_id
```

Es un `LEFT JOIN`. No hay un enlace directo entre valoraciones y selecciones.
La cadena de resolución reutilizada está encapsulada en
`silver_players_current`:

```text
players.current_national_team_id = national_teams.national_team_id
national_teams.country_id        = countries.country_id
```

La cobertura actual es completa (656.301 valoraciones con perfil), pero el
modelo y el fixture conservan valoraciones sin perfil.

## Valor histórico y metadatos de club

`historical_market_value_eur` es el importe de `player_valuations`. En la
captura va de €0 a €200.000.000, suma €1.502.990.157.999, contiene un cero
válido y no contiene nulos ni negativos. Los tests reconcilian filas, jugadores,
claves, suma, mínimo, máximo y ceros contra la entidad base.

Los campos originales llamados `current_club_*` se exponen como
`valuation_source_current_club_*`. La fuente no aporta una garantía contractual
de que representen el club histórico as-of `valuation_date`, por lo que Silver
no los renombra como tal. Se observó que 18.821 jugadores tienen más de un ID y
36.489 más de un nombre en esas columnas; 238.618 valoraciones difieren del club
del snapshot actual de players. `current_club_id_comparison_status` informa la
comparación sin elegir un ganador temporal.

`historical_market_value_eur` permanece separado de
`player_current_market_value_eur_snapshot`. El máximo, contrato, club actual,
selección, FIFA ranking y acumulados internacionales llevan sufijo `snapshot`:
son contexto de las capturas seleccionadas, no datos conocidos en la fecha de
valoración.

## Edad en la valoración

`age_at_valuation_years` usa `player_date_of_birth` y calcula años completos.
Resta un año cuando el mes/día de `valuation_date` precede al cumpleaños. El
fixture verifica explícitamente el día anterior (19 años) y el cumpleaños (20
años).

`age_at_valuation_status` distingue `calculated`, fecha de nacimiento ausente,
valoración anterior al nacimiento y perfil ausente. La captura tiene 600 filas
sin nacimiento, ninguna valoración anterior al nacimiento y edades calculadas
entre 2 y 45. El valor 2 se conserva: sin otra evidencia, Silver no impone un
umbral de plausibilidad que altere el hecho.

## Países, selección y ciudadanía

Nacimiento, ciudadanía y país de selección reutilizan IDs y estados de
resolución de `silver_players_current`. Los `country_id` siguen siendo IDs
Transfermarkt, no ISO. País o selección ausentes permanecen `NULL` con su estado
correspondiente; no se infieren desde nombres ni desde la valoración.

La fuente actual de players expone ciudadanía escalar y Stage 3 verificó que no
hay múltiples ciudadanías observadas, por lo que no existe puente que unir. El
valor escalar se replica sin expandir el grain. Si una versión futura activa
`silver_player_citizenships`, no deberá unirse directamente a este historial:
deberá agregarse o consumirse aparte para no multiplicar valoraciones.

## Procedencia y validez temporal

El modelo conserva versión, checksum, captura y run Bronze para
`player_valuations`, `players`, `national_teams`, `countries` y `clubs`, además
del procesamiento de `silver_players_current` y del actual. Los campos de
captura/version permanecen `NULL` si Bronze no los proporciona; nunca se
reemplazan por `valuation_date` o `silver_processed_at`.

Para entrenamiento histórico no son válidos como predictores as-of, salvo que
otra fuente temporal lo demuestre: club/selección actuales, ciudadanía del
snapshot, ranking FIFA actual, contrato actual, valor actual, máximo histórico
del snapshot y estadísticas internacionales acumuladas. Esta tabla tampoco
crea etiqueta predictiva, ventanas de rendimiento ni cruces con
`silver_player_match`. Esas operaciones pertenecen a Gold.

## Verificación

Los comandos y resultados reales están registrados en
`docs/silver_implementation_progress.md`. Los casos de varias valoraciones,
perfil/selección/país ausentes, cero euros y cumpleaños se prueban en un fixture
aislado, sin modificar Bronze.
