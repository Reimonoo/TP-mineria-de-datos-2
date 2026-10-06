# Matriz de trazabilidad

## Primera entrega: consigna §5.2

| Punto | Evidencia disponible |
|---|---|
| 1. Problema, usuarios y objetivos medibles | Diseño §1 |
| 2. Justificación Big Data y 5V | Diseño §2 |
| 3. Inventario/perfil/trazabilidad | Diseño §3; inventory.csv, quality.csv, landing_sha256.json |
| 4. Arquitectura de alto nivel | Diseño §4; arquitectura_v1.mmd y PNG |
| 5. Patrón justificado | Diseño §5; simulación temporal |
| 6. Matriz requisitos-componentes y 5V | Esta matriz y diseño §2 |
| 7. Diseño del Data Lake | Diseño §7 |
| 8. Flujos batch/streaming y herramientas | Diseño §8 |
| 9. Lógica MapReduce | Diseño §9; src/exploracion.py; usage_daily_reference.csv |
| 10. Supuestos/riesgos/mitigaciones | Diseño §10; DECISIONS.md |
| 11. Esfuerzo/roles/recursos | Diseño §11 |
| 12. Repo inicial y exploración | README, notebook ejecutado, evidencias; remoto pendiente de publicar |

## Objetivos de implementación posterior

| Meta | Componente propuesto | Evidencia futura |
|---|---|---|
| O1 completitud | Bronze y registro de calidad/duplicados | Reconciliación por lote con categorías excluyentes |
| O2 idempotencia | Clave natural, ledger, versión Parquet y upsert absoluto | Replay y falla parcial sin cambio de totales |
| O3 v1/v2 | Contrato explícito y conformado Silver | Casos de esquema en ambos períodos |
| O4 calidad | Reglas y quarantine | Tres reglas ejecutadas y ejemplos de rechazo |
| O5 consultas | Tablas Cassandra query-first | CQL y resultados reales |
| O6 frescura | Streaming y métricas de ejecución | Latencia llegada→serving; no timestamp histórico→hoy |
| O7 reproducibilidad | Quickstart, versiones y configuración | Corrida desde entorno limpio |

## Plan de correcciones tras feedback

Plantilla para completar el 07/10; no representa feedback ya recibido.

| Prioridad | Corrección | Responsable | Fecha objetivo | Evidencia |
|---|---|---|---|---|
| Por definir | Por completar después de revisión docente | Ramón / acuerdo del grupo | Por definir | Commit, prueba o documento actualizado |
