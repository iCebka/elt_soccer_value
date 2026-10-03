# Verificación

Estado inicial del 2 de octubre de 2026. Este archivo se actualiza con los resultados reproducibles obtenidos en el entorno local.

| Verificación | Estado | Evidencia/pendiente |
|---|---|---|
| URLs, HTTP, gzip y encabezados de 12 assets | OK | Consulta directa al host publicado; 12 respuestas HTTP 200 y primer registro gzip válido. Encabezados guardados en `config/assets.yml`. |
| README y cobertura del proveedor | OK | README oficial consultado: actualizaciones pausadas y cortes 2026-07-06 / 2026-06-28 / 2026-06-12. |
| Asset real completo pequeño | OK | `competitions.csv.gz`: 2.242 bytes, 65 filas, SHA-256 `8924ddfbc0e9989f4a42a3c32ebb6faa671614625086d7fa84353f955352ea88`, encabezado esperado y `schema_changed=false`. Se validó, no se declaró cargado. |
| Pruebas Python focalizadas | OK | `13 passed` en Python 3.12 dentro del target Docker `test`; incluye password sin `.p8`, contraseña ausente, redacción de secretos, idempotencia y errores HTTP. |
| `docker compose config --quiet` | OK | Código de salida 0; se evitó imprimir la configuración expandida. |
| Build y arranque Docker | OK | Imagen custom reconstruida con password auth. PostgreSQL y Kestra reportan `healthy`; UI publicada en `localhost:8080`; no existe montaje de claves. |
| Validación/registro de flows | OK | Ambos YAML actualizados fueron aceptados por la API de Kestra 2.0. Trigger semanal permanece desactivado. |
| Conexión y carga real Snowflake | OK | Con usuario/contraseña, `SELECT 1` y creación/verificación de `FOOTBALL.BRONZE` terminaron correctamente. El lote Kestra `469dg39ysEJrzg7DYiJFQm` cargó `competitions`: 65 leídas, 65 cargadas, estado `SUCCESS`; RAW y LATEST contienen 65 filas. |
| Preflight sin contraseña | OK (fallo esperado) | La validación local exige `SNOWFLAKE_PASSWORD`, no busca archivos `.p8` y no expone secretos. |
| Idempotencia real y fallo parcial en Snowflake | OK / parcial | El segundo lote Kestra `6LnBEQ0s5PksQD29yUf9w0` quedó `SUCCESS` con el asset `SKIPPED`; RAW siguió en 65 filas, LATEST en 65 y existe un solo checksum. La recuperación tras fallo parcial permanece cubierta por prueba local. |

Una prueba local o mock no se considera éxito end-to-end. La carga real solo queda acreditada por un lote `SUCCESS`, conteos RAW/LATEST y originales visibles en el stage de Snowflake.

