# Decisiones de orquestación, resiliencia y backfill

Este documento resume las decisiones adoptadas para cumplir los requisitos de **trigger y frecuencia de ejecución**, **manejo de errores y retries**, **backfill histórico** y **preservación del dato original en Bronze** dentro del pipeline:

`Transfermarkt → Kestra → Bronze → dbt/Silver → dbt/Gold → Spark/OBT`

## 1. Trigger y frecuencia de ejecución

El flow principal se ejecuta automáticamente mediante un `Schedule` de Kestra:

```yaml
cron: "0 8 * * 3,6"
timezone: America/Guayaquil
recoverMissedSchedules: LAST
```

La frecuencia elegida es **miércoles y sábado a las 08:00**. El dataset se consume como snapshots completos por asset, por lo que no es necesario reproducir cada ejecución perdida: `LAST` recupera únicamente la ejecución pendiente más reciente.

El flow también limita la concurrencia global:

```yaml
concurrency:
  limit: 1
  behavior: QUEUE
```

Esto evita que dos cargas modifiquen simultáneamente las mismas tablas auxiliares o la OBT.

## 2. Manejo de errores y retries

Los retries se aplican únicamente a operaciones transitorias de I/O: descarga HTTP y subida al stage de Snowflake.

```yaml
retry:
  type: exponential
  interval: PT30S
  maxInterval: PT2M
  maxAttempts: 3
  maxDuration: PT5M
  warningOnRetry: true
```

No se reintentan automáticamente fallos de validación de dbt ni errores reales de Spark, porque estos pueden indicar problemas deterministas de datos o lógica.

La carga a Snowflake usa:

```sql
ON_ERROR = ABORT_STATEMENT
```

por lo que un archivo inválido no se publica parcialmente.

Para publicar Bronze se utiliza el patrón **load table → carga completa → `SWAP`**. La tabla productiva solo cambia cuando la nueva versión ya está construida, evitando estados parciales.

El bloque `errors` del flow registra la ejecución, tareas fallidas y logs asociados. En Spark, `stderr` se redirige a `stdout` con `2>&1` para evitar que mensajes normales del runtime se clasifiquen visualmente como errores; el éxito o fallo sigue determinado por el **exit code real** del proceso.

## 3. Preservación del dato original en Bronze

Bronze actúa como capa de aterrizaje y conserva los valores de negocio sin limpieza semántica. Las columnas de la fuente se almacenan principalmente como `VARCHAR`; las conversiones de tipos, normalizaciones y reglas de calidad se realizan posteriormente en Silver.

Cada fila incorpora metadatos de ingesta:

- `_source_file`
- `_source_row_number`
- `_ingested_at`

La carga usa `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` e `INCLUDE_METADATA`, de modo que se preserva el contenido original del snapshot junto con su procedencia.

## 4. Estrategia de backfill

El dataset se distribuye como **snapshots completos por asset**. Por esta razón, el backfill natural del modelo es volver a cargar un asset completo. El selector `assets` permite reducir la granularidad de la operación a una o varias tablas concretas.

Como mecanismo adicional se mantiene un flow separado de backfill con un único intervalo:

```text
backfill_start
backfill_end
assets[]
```

No se permiten intervalos distintos por tabla dentro de una misma ejecución. Si se necesitan ventanas diferentes, se realizan ejecuciones separadas; esto mantiene una semántica y trazabilidad claras.

### Assets con dimensión temporal directa

Se aplica reemplazo por rango a:

- `games` → `date`
- `appearances` → `date`
- `player_valuations` → `date`
- `game_events` → `date`
- `game_lineups` → `date`
- `transfers` → `transfer_date`

Para estos assets se descarga igualmente el snapshot completo y se construye una tabla candidata:

```text
Bronze actual fuera del rango
          +
snapshot nuevo dentro del rango
          ↓
*_BACKFILL_MERGED
          ↓
SWAP
```

La tabla Bronze publicada no se modifica hasta que la versión candidata está completa.

### Assets sin dimensión temporal directa

`competitions`, `clubs`, `players`, `club_games`, `countries` y `national_teams` no se fuerzan a una partición temporal artificial. Si se seleccionan en el backfill, se reemplaza el **snapshot completo** mediante el mismo patrón de `SWAP`.

El backfill por fecha debe interpretarse como **reingesta de registros históricos presentes en el snapshot actual de la fuente**. No reconstruye una versión point-in-time antigua del dataset fuente.

## 5. Reconstrucción downstream después del backfill

Aunque Bronze pueda cambiar solo para un asset o un intervalo pequeño, no se implementa incrementalidad especial en las capas analíticas.

Después de cualquier modificación de Bronze se ejecuta siempre:

```text
Bronze modificado
      ↓
dbt build Silver completo
      ↓
dbt build Gold completo + tests
      ↓
Spark reconstruye OBT completa
```

Esta decisión mantiene Silver, Gold y la OBT como derivaciones coherentes del estado actual de Bronze y evita introducir lógica incremental adicional únicamente para el backfill.

## 6. Ejecución de Spark

La OBT se ejecuta localmente dentro del entorno de Kestra con recursos acotados:

```bash
SPARK_MASTER=local[4]
SPARK_DRIVER_MEMORY=2g
```

Además se fijan:

```text
spark.default.parallelism = 4
spark.sql.shuffle.partitions = 8
```

La configuración anterior con `local[*]` y `4g` podía agotar la memoria disponible del entorno Docker y terminar con `exit code 137` (`SIGKILL` por OOM). La configuración reducida sacrifica algo de tiempo de ejecución a cambio de estabilidad y completó correctamente la construcción de la OBT.

Los errores reales de Spark **no se ignoran**: un exit code distinto de cero sigue haciendo fallar el task y detiene el pipeline.
