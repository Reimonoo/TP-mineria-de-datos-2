# Guía para explicar la propuesta

## Recorrido de unos cinco minutos

1. **Problema.** “Quiero que FinOps pueda ver costos y revenue, Soporte pueda revisar tickets y SLA, y Producto pueda seguir GenAI. Hoy las fuentes están separadas y tienen problemas de calidad.”
2. **Datos.** Mostrar inventory.csv: siete CSV y 43.200 eventos. La muestra es pequeña; las 5V justifican decisiones por variedad, velocidad y veracidad, además de una escala futura hipotética.
3. **Hallazgo.** Abrir file_time_profile.csv: cada archivo mezcla 59–60 fechas. Mostrar la simulación sin presentarla como una ejecución de Spark.
4. **Decisión.** Conservar primero todos los eventos; procesar histórico batch; usar watermark/dedupe para el operativo y reconciliar late data desde Bronze.
5. **Calidad.** Mostrar tres ejemplos: value como texto se convierte, costo negativo se conserva con flag, registro con clave inválida iría a quarantine. En esta muestra las claves no tienen huérfanos.
6. **Resultado.** MapReduce obtiene 11.050 claves y 147433.9778 USD; valida costo por clave y conteos. Es evidencia para comparar contra Spark después.
7. **Límite.** Esta entrega diseña y explora; no dice que Cassandra o un pipeline streaming estén ejecutados. Mostrar próximos pasos.

## Preguntas probables

**¿Por qué Spark con solo 13 MB?** La muestra no exige Big Data por volumen. Spark es el stack de la consigna y permite probar un diseño portable a una escala mayor; hoy la complejidad principal es la combinación de fuentes, calidad y tiempo.

**¿Por qué no poner watermark de 60 días y listo?** Ampliar el margen puede incluir el histórico, pero aumenta estado y no soluciona por sí mismo reintentos, duplicados fuera de horizonte ni actualizaciones de serving. Separar backfill y operativo hace explícitas sus necesidades.

**¿El watermark descarta siempre todo lo viejo?** No. Depende de operadores con estado y su ejecución; un append raw no tiene el mismo comportamiento. La simulación estima eventos debajo de un umbral y tiene supuestos definidos.

**¿Qué diferencia hay entre checkpoint e idempotencia?** Checkpoint registra progreso de una query. Si un sink personalizado falla parcialmente, puede recibir de nuevo un lote. Su lógica de escritura debe producir el mismo resultado al repetirlo.

**¿Parquet tiene upsert?** No. La propuesta reconstruye particiones y publica versiones validadas con un solo writer. Cassandra sí recibe escrituras por primary key, pero un upsert no elimina rankings viejos ni vuelve atómica la escritura en varios sistemas.

**¿Por qué no borrar los costos negativos?** Pueden ser ajustes; sin contrato de negocio no se sabe. Se conservan y se marcan para no alterar silenciosamente totales financieros.

**¿Por qué no poner cero a todos los nulos?** Cero significa medición conocida de cero. Ausente significa desconocido o no disponible en esa versión; por eso publico suma conocida y completitud.

**¿Qué cambiaste respecto de tus compañeros?** Reproduje sus hallazgos en el dataset original y ajusté parsing de tags, conservación de nulos, explicación del watermark y mecanismo de escritura Parquet. Los aportes compartidos están referenciados.

**¿Qué falta acordar?** FX/créditos/negativos, escala NPS/CSAT, severidades críticas, entorno y versiones para serving. No son cosas que el perfilado pueda decidir por sí solo.

Antes de la entrega, revisar el documento y adaptar las frases a cómo lo explicarías oralmente. La guía sirve para entender y practicar, no como evidencia de una defensa ya realizada.
