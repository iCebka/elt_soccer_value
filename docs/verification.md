# Verificación

Resultados del 2 de octubre de 2026 para el cambio mensual/condicional. Una prueba con fixture no se presenta como integración real.

| Comprobación | Resultado | Evidencia |
|---|---|---|
| Pruebas Python | OK | `27 passed` con Python 3.12. Incluyen Schedule/manual/backfill lógico, primera carga, ETag igual/diferente, HEAD/GET 304, ausencia de validadores, timeout/429/5xx, límite HTTP de tres solicitudes, fallo sin checkpoint, recuperación sin duplicados, auditoría y redacción de credenciales. |
| Compose | OK | `docker compose config --quiet` terminó con código 0 sin imprimir variables expandidas. |
| Vista previa de backfill | OK | Intervalo 2026-07-01 a 2026-09-02 produjo exactamente tres fechas lógicas: 1 de julio, agosto y septiembre a las 06:00 `-05:00`; no llamó a Kestra. |
| R2 HEAD | OK | `competitions.csv.gz` respondió `200` con ETag entre comillas, `Last-Modified` y `Content-Length`. |
| R2 condicional | OK | Se obtuvo el ETag dinámicamente del HEAD, se reenvió sin normalizar en `If-None-Match` y R2 respondió `304`. El valor no está hardcodeado. |
| Schedule estático | OK | YAML: `0 6 1 * *`, `America/Guayaquil`, `disabled: true`, `recoverMissedSchedules: NONE`, concurrency 1. Compose también fija recuperación global `NONE`. |
| Docker/Kestra actual | OK | Se reconstruyó la imagen, PostgreSQL quedó saludable, Kestra `2.0.4` quedó listo y su API aceptó ambos flows. El flow principal registrado (revisión 2) reportó `0 6 1 * *`, `America/Guayaquil`, `disabled: true`, `recoverMissedSchedules: NONE` y concurrencia 1. No se activó el trigger ni se creó ningún backfill. |
| Snowflake preflight/migración | OK | Con las credenciales locales por contraseña, `SELECT 1` terminó correctamente y `check` añadió las columnas de auditoría con migraciones no destructivas. No apareció `002043`. |
| Integración condicional real | OK | `competitions`: un intento inicial expuso y dejó auditado un fallo `001065` al convertir un `NULL` histórico; no adelantó el checkpoint. Tras tipar el bind, `asset-b` recuperó con HEAD+GET y `SKIPPED_UNCHANGED` por SHA-256 (2 solicitudes). La ejecución final obtuvo HEAD `304`, no hizo GET y quedó `SKIPPED_UNCHANGED` (1 solicitud); una consulta posterior confirmó 65 filas RAW y 1 checksum. |
| Integración Kestra real | OK | Las ejecuciones manuales `7KwfRbxm1Lo5Ucdf3FaT9Q` y `68xVTV9l01dyqVk2YJU77T`, limitadas a `competitions`, terminaron `SUCCESS`. La última auditoría quedó `SKIPPED_UNCHANGED`, HEAD `304`, GET nulo, 0 filas cargadas y 1 intento HTTP. Después de repetir, RAW continuó en 65 filas y 1 checksum. |
| Secretos en logs | OK | Se compararon los logs del contenedor con `SNOWFLAKE_PASSWORD` y `KESTRA_PASSWORD` sin imprimir sus valores; ninguna apareció. El endpoint `Disable` también se verificó y el Schedule quedó desactivado. |

Comandos usados para repetir la validación Kestra:

```powershell
docker compose build
docker compose up -d --force-recreate postgres kestra
.\scripts\wait-kestra.ps1
.\scripts\register-flows.ps1
docker compose exec -T kestra /opt/transfermarkt/.venv/bin/python -m ingestion.cli --config /opt/transfermarkt/config/assets.yml check
.\scripts\run-flow.ps1 -Mode incremental -Assets competitions
.\scripts\run-flow.ps1 -Mode incremental -Assets competitions
```

La primera ejecución debe ser `SUCCESS` solo si existe una versión nueva o aún no hay una exitosa. La segunda debe ser `SKIPPED_UNCHANGED`, no hacer GET y mantener sin cambios el conteo RAW. Después del preflight puede activarse el cron con `.\scripts\manage-schedule.ps1 -Action Enable`.
