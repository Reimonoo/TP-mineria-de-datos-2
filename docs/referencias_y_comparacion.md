# Referencias y comparación de propuestas

Consulta: 06/10/2026. Propuestas del mismo grupo, revisadas como insumo para comparar diseños. La redacción y el código de este repositorio se elaboraron para esta versión; los resultados numéricos se recalcularon sobre los archivos del profesor.

| Referencia | Aporte aprovechado | Ajuste en esta propuesta |
|---|---|---|
| [Sebastián](https://github.com/SebGalvaleira/mineria-datos-ii-parcial) | Exploración separada por temas, hallazgo temporal, MapReduce y función compartida entre batch/streaming | Simulación expresada como estimación; histórico separado; faltantes contados; publicación Parquet concreta |
| [María](https://github.com/MariaLopez1999/Miner-a-de-Datos-II/tree/main) | Documento compacto, matriz y separación de primera/segunda entrega | Evidencia ejecutable, política temporal específica para este dataset y Quickstart sin rutas personales |
| [David](https://github.com/Davoo0o/ISTEA_Mineria_de_datos_II/tree/main) | Bronze por llegada, evaluación de small files, precisión entre watermark y append | Validación del parser CSV; no concluir tags corruptos por una lectura incompatible; confirmar tipos desde JSON original |

Revisiones consultadas: Sebastián `960ec1df537d26e86f78b9a5a3f73f9b02f6c6d2`; María `4e63b9adbb308ba699b6b0929ecf2c6d59916fe5`; David `6d6db6fa9b10ec34a4d02d465f53ca54fdb5b007` (árboles main al momento de consulta; los repos pueden cambiar).

## Diferencias verificadas

- `tags_json`: csv.DictReader interpreta 317 arrays válidos y 83 vacíos. Un resultado de cero parseables debe investigarse desde opciones de CSV, antes de usar regex para “reparar” datos.
- `value`: leer JSON original conserva 1.309 strings convertibles. Leer todos los primitivos como string permite castear, pero deja de servir para identificar su tipo original.
- CSAT: `>5` y `fuera de 1–5` son reglas distintas. Esta versión detecta 40 fuera de 1–5, sujeto a confirmar escala; no descarta el ticket completo.
- Spikes: un umbral >100 USD arroja 48 eventos; otro umbral puede arrojar 49. No son necesariamente resultados contradictorios: se debe explicitar la regla.
- Linaje Gold: un agregado puede combinar muchos archivos. Se mantiene relación por corrida y fuentes, no se promete un único source_file por fila agregada.
- FX: la similitud de montos ARS con USD no demuestra que estén ya convertidos. Se conserva la moneda declarada y se señala el contrato pendiente.

## Fuentes primarias

- Consigna oficial del profesor y README del dataset sintético adjuntos al proyecto.
- [Guía de Structured Streaming, Spark 3.5.6](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html): event-time, watermark, deduplicación y garantías de foreachBatch. Es referencia de diseño, no una afirmación de versión instalada.
- [API dropDuplicatesWithinWatermark](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html).

El objetivo de la comparación es consolidar decisiones del grupo y verificar evidencia. No se presentan implementaciones ajenas ni feedback pendiente como trabajo ejecutado de esta versión.
