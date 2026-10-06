# Registro de decisiones

Fecha: 06/10/2026. Estado general: diseño propuesto para primera entrega.

| ID | Decisión | Motivo y alternativa |
|---|---|---|
| D01 | Híbrido batch/streaming con reconciliación | Dos ritmos reales; solo batch incumple y Kappa agrega complejidad a maestros |
| D02 | Captura Bronze sin descarte temporal | Preservar histórico antes de operadores con estado |
| D03 | Separar backfill del camino operativo | 59–60 fechas por archivo invalidan asumir llegada cronológica |
| D04 | Watermark event-time propuesto de 2 h para la demo operativa | Validar con late data; no es la retención de Bronze ni garantía global de dedupe |
| D05 | Dedupe definitivo por event_id en Silver | Checkpoint y dedupe acotado no resuelven todo replay |
| D06 | Bronze por ingest_date; Silver/Gold mensual en la muestra | Evitar sobreparticionar; revisar a escala mayor |
| D07 | Publicación versionada en Parquet, un writer | Parquet carece de MERGE; no prometer transacciones o upsert nativo |
| D08 | Flags para negativos y ambigüedades de negocio | No eliminar costos/subtotales sin confirmar su semántica |
| D09 | Métricas conocidas más conteo de faltantes | Nulo no equivale a cero; mantener completitud |
| D10 | Parser CSV correcto antes de reparar tags | 317 valores no vacíos son JSON válido con lectura estándar |
| D11 | FX efectivo=1 en USD; otras monedas según contrato provisto | Preservar original; no inferir moneda por magnitud |
| D12 | Serving query-first con buckets mensuales | Evitar ALLOW FILTERING y particiones sin límite |
| D13 | Exploración inicial en Python estándar | Evidencia reproducible con pocas dependencias; no reemplaza Spark obligatorio posterior |
| D14 | Documento directo y guía de defensa | Relacionar cada decisión con una evidencia y distinguir medición de supuesto |

Pendientes: contratos financieros/satisfacción, critical severity, entorno Spark/Java/conector, retención definitiva, SLA de latencia y mecanismo de publicación persistente. Registrar cada cambio posterior con fecha, motivo y evidencia.
