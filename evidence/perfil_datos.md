# Perfil del dataset · Evidencia ejecutada

Generado a partir de los archivos originales con `python src/exploracion.py`. Esta exploración usa Python estándar; no representa una corrida Spark.

## Inventario

| Fuente | Filas | Columnas | Clave |
|---|---:|---:|---|
| customers_orgs | 80 | 11 | org_id |
| users | 800 | 7 | user_id |
| resources | 400 | 7 | resource_id |
| support_tickets | 1000 | 8 | ticket_id |
| marketing_touches | 1500 | 7 | touch_id |
| nps_surveys | 92 | 4 | org_id+survey_date |
| billing_monthly | 240 | 8 | invoice_id |
| usage_events | 43200 | 13 | event_id |

## Calidad medida

Los conteos no se deben sumar entre sí: una fila puede activar varias reglas. El tratamiento es una propuesta de diseño, no una transformación ya aplicada a las fuentes.

| Fuente | Regla | Casos | % | Propuesta |
|---|---|---:|---:|---|
| customers_orgs | duplicate_key | 0 | 0.0 | dedupe |
| users | duplicate_key | 0 | 0.0 | dedupe |
| users | orphan_org | 0 | 0.0 | quarantine |
| resources | duplicate_key | 0 | 0.0 | dedupe |
| resources | orphan_org | 0 | 0.0 | quarantine |
| support_tickets | duplicate_key | 0 | 0.0 | dedupe |
| support_tickets | orphan_org | 0 | 0.0 | quarantine |
| marketing_touches | duplicate_key | 0 | 0.0 | dedupe |
| marketing_touches | orphan_org | 0 | 0.0 | quarantine |
| nps_surveys | duplicate_key | 0 | 0.0 | dedupe |
| nps_surveys | orphan_org | 0 | 0.0 | quarantine |
| billing_monthly | duplicate_key | 0 | 0.0 | dedupe |
| billing_monthly | orphan_org | 0 | 0.0 | quarantine |
| usage_events | duplicate_event_id | 0 | 0.0 | dedupe |
| usage_events | value_string | 1309 | 3.03 | cast_controlado |
| usage_events | value_null | 877 | 2.03 | conservar_nulo_y_contar |
| usage_events | value_invalid_cast | 0 | 0.0 | quarantine_metrica |
| usage_events | unit_null | 2075 | 4.803 | inferir_desde_metric_con_flag |
| usage_events | negative_cost | 216 | 0.5 | flag_sin_borrar |
| usage_events | cost_gt_100_usd | 48 | 0.111 | indicador_exploratorio_no_umbral_final |
| usage_events | orphan_org | 0 | 0.0 | quarantine |
| usage_events | orphan_resource | 0 | 0.0 | quarantine |
| usage_events | resource_org_mismatch | 0 | 0.0 | quarantine |
| usage_events | resource_service_mismatch | 0 | 0.0 | flag |
| resources | invalid_nonempty_tags_json | 0 | 0.0 | validar_parser_csv_antes_de_reparar |
| resources | tags_null | 83 | 20.75 | preservar_ausencia |
| billing_monthly | negative_subtotal | 13 | 5.417 | flag_pendiente_regla_negocio |
| billing_monthly | credits_null | 137 | 57.083 | cero_con_flag_supuesto |
| billing_monthly | usd_fx_not_one | 160 | 66.667 | fx_efectivo_uno_con_flag |
| users | login_before_created | 232 | 29.0 | flag |
| customers_orgs | nps_null | 11 | 13.75 | conservar_nulo |
| customers_orgs | nps_outside_minus100_100 | 1 | 1.25 | flag_escala_a_confirmar |
| nps_surveys | nps_null | 19 | 20.652 | conservar_nulo |
| nps_surveys | nps_outside_minus100_100 | 0 | 0.0 | flag_escala_a_confirmar |
| support_tickets | open_ticket | 240 | 24.0 | estado_valido |
| support_tickets | csat_null | 254 | 25.4 | denominador_solo_validos |
| support_tickets | csat_outside_1_5 | 40 | 4.0 | flag_escala_a_confirmar |

## Esquema y tiempo

10.800 eventos v1 y 32.400 v2; período 03/07–31/08/2025. Cada archivo tiene 360 eventos y 59 o 60 fechas distintas. El JSON original conserva 1.309 strings numéricos, todos convertibles, y 877 nulos.

## Simulación temporal

Orden lexicográfico, un archivo/lote, umbral calculado con máximo timestamp de lotes anteriores. Es una aproximación del horizonte temporal, no medición de descartes de un operador Spark.

| Margen (horas) | Debajo del umbral | % |
|---|---:|---:|
| 1 | 42801 | 99.076 |
| 2 | 42783 | 99.035 |
| 24 | 42118 | 97.495 |
| 168 | 37831 | 87.572 |
| 720 | 21421 | 49.586 |
| 1440 | 0 | 0.0 |

## MapReduce de referencia

- 43.200 eventos procesados; 42.572 pares después del combiner.
- 11.050 claves `(org_id, usage_date, service)`.
- Costo firmado total: **147433.9778 USD**.
- Validación ejecutada: costo por clave contra agregación directa y suma del conteo de eventos.
- Valores ausentes no se inventan: se publican sumas conocidas y conteos de faltantes.

`usage_daily_reference.csv` es una salida exploratoria calculada desde Landing, no un mart Gold implementado. `landing_sha256.json` permite verificar que la copia de cada archivo coincide con el ZIP original.
