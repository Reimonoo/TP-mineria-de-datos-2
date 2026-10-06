# Cloud Provider Analytics

**Minería de Datos II · ISTEA · 2C 2026**  
**Autor de esta propuesta individual:** Ramón Ojea  
**Profesor:** Diego Mosquera · **Primera entrega:** 07/10/2026, 19:00 h

El objetivo es convertir datos de uso, facturación y soporte de un proveedor de nube en información que sirva para controlar costos, priorizar tickets y entender el uso de los servicios.

Esta versión cubre **diseño y fundación de datos**. Incluye exploración ejecutable, evidencia, arquitectura propuesta, Data Lake, MapReduce y plan de implementación. PySpark, Structured Streaming y Cassandra/AstraDB forman parte del diseño; su pipeline operativo corresponde a la segunda entrega.

## Por dónde empezar

- [Documento principal de diseño](docs/diseno_entrega1.md): cubre los 12 puntos de la consigna.
- [Evidencia de exploración](evidence/perfil_datos.md): resultados medidos sobre el dataset original.
- [Notebook ejecutado](notebooks/01_exploracion_y_mapreduce.ipynb): lectura, calidad, simulación temporal y MapReduce.
- [Decisiones](DECISIONS.md), [matriz de requisitos](docs/matriz_requisitos.md) y [guía de defensa](docs/guia_defensa.md).
- [Comparación de enfoques y referencias](docs/referencias_y_comparacion.md): aportes discutidos con los compañeros y ajustes de esta propuesta.

## Qué encontré en los datos

Los 120 archivos de eventos mezclan 59 o 60 fechas cada uno. Por eso propongo conservar primero todos los eventos y separar el histórico del procesamiento operativo con estado. Un watermark corto aplicado directamente al histórico dejaría gran parte de los datos fuera de su horizonte temporal.

También aparecen 1.309 valores numéricos como texto, 877 valores nulos, 216 costos negativos y 160 facturas USD con un tipo de cambio distinto de uno. Son problemas distintos: algunos se pueden corregir de forma controlada y otros necesitan una definición de negocio.

## Reproducir la evidencia

Requisito mínimo: **Python 3.10 o superior**. Validado con Python 3.12.14 en Linux. La exploración usa únicamente la biblioteca estándar; no necesita Java, Spark, credenciales ni conexión externa.

Desde la raíz del repositorio:

```bash
python src/exploracion.py
```

Salida esperada: 43.200 eventos, 120 archivos, 60 fechas, 10.800 eventos v1 y 32.400 v2. La agregación de referencia devuelve 11.050 claves y 147433.9778 USD. El script regenera los CSV, el resumen y los hashes en `evidence/`; los originales de `data/datalake/landing/` permanecen intactos.

Para usar otra carpeta o escribir las evidencias en otro destino:

```bash
python src/exploracion.py --landing /ruta/datalake/landing --output /ruta/evidencias
```

El notebook puede abrirse en Jupyter o Colab. En Colab hay que disponer de la carpeta completa del repositorio y ejecutar desde esa raíz; el notebook busca `src/exploracion.py` en el directorio actual o en el padre. Dependencia opcional local: `pip install -r requirements-notebooks.txt`.

## Estructura y convenciones

| Ruta | Contenido |
|---|---|
| `docs/` | Diseño, diagrama Mermaid e imagen, trazabilidad, riesgos y defensa |
| `src/exploracion.py` | Perfilado y MapReduce de referencia en Python |
| `notebooks/` | Recorrido ejecutado de la exploración |
| `evidence/` | CSV medidos, resumen JSON, hashes y validación |
| `data/datalake/landing/` | Copia fiel del dataset sintético del profesor |

Nombres `snake_case`, fechas UTC, claves naturales documentadas y montos monetarios con precisión decimal. Landing es inmutable. Las reglas dudosas se registran con un flag; no se eliminan registros para mejorar un indicador.

## Próximas entregas y límites

Para el 18/11 se implementarán batch de tres maestros, streaming con checkpoint/watermark/dedupe/late data, Silver, tres features, tres reglas de calidad, un mart Gold y dos consultas reales en Cassandra. Para el 09/12 se integrarán los cinco dominios/consultas requeridos y el material de presentación.

El CSV `usage_daily_reference.csv` es evidencia de cálculo sobre Landing, **no una capa Gold productiva**. La simulación del watermark no mide descartes reales de Spark. Los objetivos de latencia son metas propuestas, no benchmarks. La idempotencia distribuida y el serving todavía deben implementarse y demostrarse.

## Datos y referencias

Los datos incluidos son sintéticos y provienen de `cloud_provider_challenge_dataset_v1.zip`. La consigna oficial es `Consigna_Proyecto_Integrador_Mineria_de_Datos_II_ISTEA_2C_2026.pdf`, especialmente §§5 y 9.1. Se revisaron las propuestas de Sebastián, María y David; se documentan los aportes y las diferencias en [referencias](docs/referencias_y_comparacion.md).
