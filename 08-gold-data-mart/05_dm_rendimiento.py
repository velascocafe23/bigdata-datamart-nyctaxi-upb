# Databricks notebook source
# MAGIC %run ../00-setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Parte 5: Rendimiento y diseño físico
# MAGIC
# MAGIC **Consulta:** top 20 pares zona de origen → zona de destino con más viajes, excluyendo las zonas 264 y 265 en ambos extremos. Por cada par: viajes, distancia promedio, `total_amount` promedio y duración promedio.
# MAGIC
# MAGIC Se escribe en dos versiones que deben devolver **el mismo resultado**:
# MAGIC - **A. Silver (tabla ancha):** agrupa por `PULocationID`/`DOLocationID` y usa los nombres de zona que silver ya trae unidos.
# MAGIC - **B. Estrella:** `fact_trips` + `dim_zone` en sus dos roles; la exclusión se hace con `is_special_zone`.

# COMMAND ----------

from pyspark.sql import functions as F
import time

q_silver = f"""
    SELECT PULocationID AS pickup_zone_id, pickup_zone,
           DOLocationID AS dropoff_zone_id, dropoff_zone,
           COUNT(*)                         AS viajes,
           ROUND(AVG(trip_distance), 4)     AS distancia_promedio,
           ROUND(AVG(total_amount), 4)      AS total_amount_promedio,
           ROUND(AVG(trip_duration_min), 4) AS duracion_promedio_min
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE PULocationID NOT IN (264, 265)
      AND DOLocationID NOT IN (264, 265)
    GROUP BY PULocationID, pickup_zone, DOLocationID, dropoff_zone
    ORDER BY viajes DESC, pickup_zone_id, dropoff_zone_id
    LIMIT 20
"""

q_estrella = f"""
    SELECT f.pickup_zone_key  AS pickup_zone_id,  zp.zone AS pickup_zone,
           f.dropoff_zone_key AS dropoff_zone_id, zd.zone AS dropoff_zone,
           COUNT(*)                           AS viajes,
           ROUND(AVG(f.trip_distance), 4)     AS distancia_promedio,
           ROUND(AVG(f.total_amount), 4)      AS total_amount_promedio,
           ROUND(AVG(f.trip_duration_min), 4) AS duracion_promedio_min
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zp ON f.pickup_zone_key  = zp.zone_key
    JOIN {T_DM_DIM_ZONE} zd ON f.dropoff_zone_key = zd.zone_key
    WHERE NOT zp.is_special_zone
      AND NOT zd.is_special_zone
    GROUP BY f.pickup_zone_key, zp.zone, f.dropoff_zone_key, zd.zone
    ORDER BY viajes DESC, pickup_zone_id, dropoff_zone_id
    LIMIT 20
"""

def medir(nombre, sql, repeticiones=3):
    """Ejecuta la consulta varias veces y devuelve (resultado, tiempos). La 1.ª corrida incluye 'calentamiento'."""
    tiempos = []
    for _ in range(repeticiones):
        t0 = time.time()
        filas = spark.sql(sql).collect()
        tiempos.append(round(time.time() - t0, 2))
    print(f"{nombre:<10} tiempos (s): {tiempos}   mejor: {min(tiempos)} s")
    return filas, tiempos

res_silver,   t_silver   = medir("Silver",   q_silver)
res_estrella, t_estrella = medir("Estrella", q_estrella)

# Ambas versiones deben devolver exactamente lo mismo
df_s, df_e = spark.createDataFrame(res_silver), spark.createDataFrame(res_estrella)
assert df_s.count() == df_e.count() == 20, "Ambas deben devolver 20 filas"
assert df_s.exceptAll(df_e).count() == 0 and df_e.exceptAll(df_s).count() == 0, \
    "Las dos versiones NO devuelven el mismo resultado"
print("✓ Silver y estrella devuelven el mismo top 20")

display(df_e)

# COMMAND ----------

print("════════ PLAN — SILVER ════════")
spark.sql(q_silver).explain("formatted")
print("\n\n════════ PLAN — ESTRELLA ════════")
spark.sql(q_estrella).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Resultados
# MAGIC
# MAGIC | Versión | Corridas (s) | Mejor | Joins en el plan |
# MAGIC |---|---|---|---|
# MAGIC | A. Silver (tabla ancha) | 2,32 · 1,76 · 1,60 | 1,60 s | ninguno |
# MAGIC | B. Estrella | 1,56 · 1,30 · 1,32 | **1,30 s** | 2 × `PhotonBroadcastHashJoin` |
# MAGIC
# MAGIC Las dos devuelven **exactamente el mismo top 20** (verificado con `exceptAll` en ambos sentidos). El par con más viajes es Upper East Side South → Upper East Side North (280.091 viajes, 1,04 mi, $15,75): los viajes cortos dentro del Upper East Side dominan la demanda.
# MAGIC
# MAGIC **¿Dónde aparecen los broadcast joins?** Solo en la estrella: pasos **(8)** (`dropoff_zone_key = zone_key`) y **(16)** (`pickup_zone_key = zone_key`), uno por rol de `dim_zone`. En los dos, la dimensión se filtra al leerla (`NOT is_special_zone`, pasos 3 y 11), queda en 263 filas y 2 columnas, y se envía completa a cada ejecutor (`EXECUTOR_BROADCAST`, pasos 6 y 14). La tabla de hechos **no se reparte (shuffle)** para el join. Además, el optimizador hizo una **agregación parcial antes de los joins** (paso 2): agrupa los 39,7 millones de viajes por par de llaves y une con la dimensión solo los pares resultantes. Silver no tiene joins, porque los nombres ya vienen copiados en cada fila.
# MAGIC
# MAGIC ### Preguntas
# MAGIC
# MAGIC **¿La estrella fue más rápida que la tabla ancha de silver? ¿Por qué?**
# MAGIC Sí, un 19 % (1,30 s vs 1,60 s), aunque tiene dos joins más. Lo que pesa es cuánto se lee y cómo se agrupa, no cuántos joins hay:
# MAGIC - **Lee menos datos:** la estrella lee 5 columnas numéricas (`INT` y `DOUBLE`). Silver lee 7, incluidas **dos de texto** (`pickup_zone`, `dropoff_zone`) repetidas en 39,7 millones de filas, más los IDs como `BIGINT`.
# MAGIC - **Agrupa por enteros, no por texto:** el `GROUP BY` de silver incluye los nombres de zona (texto), y calcular el hash de un texto cuesta más que el de un entero. La estrella agrupa solo por llaves enteras y pone los nombres al final, sobre unos pocos miles de filas.
# MAGIC - **Los joins son baratos:** la dimensión tiene 266 filas, se envía a cada ejecutor (broadcast) y se une después de agregar.
# MAGIC - **Archivos más grandes:** silver está particionada por `pickup_date` (365 particiones, muchos archivos pequeños). `fact_trips` se escribió sin particionar, en menos archivos y más grandes.
# MAGIC
# MAGIC La diferencia es pequeña y en serverless hay variación entre corridas (la primera incluye calentamiento), así que la conclusión es "la estrella no es más lenta", más que "es mucho más rápida". **Aunque no ganara en velocidad, el mart aporta:**
# MAGIC - **Reglas de negocio en un solo lugar:** "excluir zonas especiales" es `is_special_zone`, "propina solo con tarjeta" es `tip_is_recorded`. En silver cada consulta repite `NOT IN (264, 265)` o `payment_type = 1`, y basta que alguien lo olvide para que los números no cuadren.
# MAGIC - **Nombres legibles y consistentes** para Finanzas, y dimensiones que se corrigen en un solo sitio (un nombre de zona o una descripción de pago se cambian en la dimensión, no en 40 millones de filas).
# MAGIC - **Gobierno:** Finanzas lee vistas sin acceso a silver (Parte 3.3), con constraints que documentan el modelo y linaje silver → fact → vistas.
# MAGIC - **Preguntas nuevas sin tablas nuevas:** las seis preguntas de la Parte 4 salieron de la misma estrella, sin crear una tabla ni una tarea más en el workflow.
# MAGIC - **Calendario completo:** `dim_date` trae festivos y días sin viajes, cosa que silver no puede ofrecer.
# MAGIC
# MAGIC **¿Cuándo seguirías creando una tabla agregada aunque tengas el mart?**
# MAGIC Cuando la **misma** agregación se consulta muchas veces y la latencia o el costo importan más que la flexibilidad. Por ejemplo, la demanda por zona × fecha × hora que alimenta un dashboard operativo consultado cientos de veces al día: cada consulta al mart vuelve a leer y agregar 40 millones de filas, mientras que una tabla precalculada tiene a lo sumo unas pocas cientos de miles de filas y responde en milisegundos con un costo de cómputo mínimo. También cuando hay un SLA de latencia (aplicaciones, APIs), cuando un consumidor necesita una "foto" estable de un cierre mensual, o cuando la agregación es cara (ventanas, percentiles, conteos distintos). La regla clave es que esa tabla **se construya desde el mart y no desde silver**, para que herede las mismas reglas de negocio y las mismas llaves, y vaya como una tarea más en el workflow, después de la conciliación. En Databricks también puede ser una **materialized view** sobre la estrella, que se refresca sola.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Diseño físico de `fact_trips`: **liquid clustering** con `CLUSTER BY (pickup_date_key, pickup_zone_key)`
# MAGIC
# MAGIC **Filtros que más usa la Parte 4:**
# MAGIC
# MAGIC | Pregunta | Filtra o agrupa por |
# MAGIC |---|---|
# MAGIC | P1 Revenue mensual por borough | **fecha** (año/mes) + **zona pickup** |
# MAGIC | P2 Ticket a aeropuertos, festivo vs normal | **fecha** (festivo) + zona dropoff |
# MAGIC | P3 Tarifa JFK | tipo de tarifa + zona pickup/dropoff |
# MAGIC | P4 Propinas por franja y fin de semana | **fecha** (fin de semana) + hora + forma de pago |
# MAGIC | P5 Mezcla de pagos por mes | **fecha** (mes) + forma de pago |
# MAGIC | P6 Recargos por borough | **fecha** (período) + **zona pickup** |
# MAGIC | Parte 5 Top pares | **zona pickup** + zona dropoff |
# MAGIC
# MAGIC La **fecha** aparece en 5 de las 6 preguntas y la **zona de pickup** en 4. Son las dos llaves de clustering.
# MAGIC
# MAGIC **¿Por qué liquid clustering y no las otras opciones?**
# MAGIC - **Particionar por fecha:** serían 365 particiones de unas 109 mil filas cada una. Eso genera muchos archivos pequeños, que es justo el problema que ya tiene silver. Particionar por mes deja pocas particiones, pero no ayuda con la zona. Además, cambiar la partición después obliga a reescribir toda la tabla.
# MAGIC - **`ZORDER BY`:** ordena por varias columnas, pero hay que volver a correrlo completo en cada `OPTIMIZE` y no es incremental.
# MAGIC - **Liquid clustering:** ordena los datos por las dos llaves sin crear particiones físicas, así que no genera archivos pequeños. Funciona bien con la zona (266 valores) y la fecha (365). Es **incremental**: `OPTIMIZE` solo reagrupa lo nuevo, y las llaves se pueden cambiar con `ALTER TABLE` sin reescribir la tabla. No se combina con particionado ni con ZORDER, y `fact_trips` no usa ninguno de los dos.
# MAGIC
# MAGIC **Cómo ayuda si los filtros son sobre la dimensión** (por ejemplo `d.month = 3` o `zp.borough = 'Manhattan'`): Databricks resuelve primero qué llaves cumplen el filtro en la dimensión y con ellas descarta archivos de la tabla de hechos (*dynamic file pruning*). Eso solo funciona bien si los archivos están agrupados por esas llaves.

# COMMAND ----------

spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} CLUSTER BY (pickup_date_key, pickup_zone_key)")
spark.sql(f"OPTIMIZE {T_DM_FACT_TRIPS}")

detalle = spark.sql(f"DESCRIBE DETAIL {T_DM_FACT_TRIPS}").select(
    "name", "clusteringColumns", "partitionColumns", "numFiles", "sizeInBytes"
)
display(detalle)

assert detalle.first()["clusteringColumns"] == ["pickup_date_key", "pickup_zone_key"], "El clustering no quedó aplicado"
print("✓ fact_trips con liquid clustering por (pickup_date_key, pickup_zone_key)")

# COMMAND ----------

_, t_estrella_cluster = medir("Estrella (con clustering)", q_estrella)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Resultado del clustering:** `fact_trips` quedó con `clusteringColumns = [pickup_date_key, pickup_zone_key]`, sin particiones, en **12 archivos de unos 59 MB** (712 MB en total). Es un tamaño de archivo sano para Delta, comparado con las 365 particiones de silver.
# MAGIC
# MAGIC **Tiempo del top 20 después del clustering: 1,35 s** (antes 1,30 s), dentro de la variación normal entre corridas. Es lo esperado: esta consulta **no filtra** por fecha ni por una zona de origen concreta (solo excluye 264/265), así que igual tiene que leer toda la tabla, y el clustering no puede descartar archivos. El beneficio aparece en las consultas de la Parte 4 que sí filtran: un mes concreto (P1, P5), un período (P6) o un borough de origen. Ahí Databricks lee solo los archivos cuyo rango de `pickup_date_key` y `pickup_zone_key` puede contener los datos pedidos, en lugar de los 12.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Parte 6: Orquestación del data mart
# MAGIC
# MAGIC Tareas que se agregarían a `07-workflow/resources/nyctaxi_job.yml` (no se modifica el bundle; este es el diseño).
# MAGIC
# MAGIC ```
# MAGIC  bronze_trips ──► silver_clean ──┐
# MAGIC                                  ├──► silver_enriched ──┬──► gold_revenue_by_zone   ┐
# MAGIC  bronze_zones ───────────────────┘                      ├──► gold_daily_metrics     │ (existentes,
# MAGIC                                                         ├──► gold_hourly_demand     │  en paralelo)
# MAGIC                                                         ├──► gold_ml_features ──► train ──► register ──► batch_inference
# MAGIC                                                         │
# MAGIC                                                         │   ════════ DATA MART (nuevo, en paralelo con gold y ML) ════════
# MAGIC                                                         │
# MAGIC                                                         └──► dm_dimensiones ──► dm_fact_trips ──► dm_constraints_vistas
# MAGIC                                                               (6 dimensiones)   (hechos +         (PK/FK, vistas,
# MAGIC                                                                                  CONCILIACIÓN ✋)   grants)
# MAGIC ```
# MAGIC
# MAGIC | Tarea nueva | Notebook | Depende de | Por qué |
# MAGIC |---|---|---|---|
# MAGIC | `dm_dimensiones` | `08-gold-data-mart/01_dm_dimensiones` | `silver_enriched` | `dim_date` toma el rango de fechas de silver, y `dim_zone` sale de `bronze_zones`, que ya es dependencia de `silver_enriched` |
# MAGIC | `dm_fact_trips` | `08-gold-data-mart/02_dm_fact_trips` | `dm_dimensiones` | Las llaves foráneas se resuelven contra las dimensiones ya escritas (left join + `-1`). **La conciliación es la última celda de esta tarea**: si no cuadra, la tarea falla |
# MAGIC | `dm_constraints_vistas` | `08-gold-data-mart/03_dm_constraints_vistas_permisos` | `dm_fact_trips` | Las FK necesitan las PK de las dimensiones, y `RELY` solo se declara después de que la conciliación validó cero duplicados y cero huérfanas. Las vistas y los grants van al final |
# MAGIC
# MAGIC **Qué corre en paralelo:** toda la rama del mart arranca apenas termina `silver_enriched`, en paralelo con las cuatro tareas `gold_*` y la cadena de ML. Ninguna de esas ramas depende de la otra. Dentro del mart el orden es secuencial, porque cada paso necesita el anterior: dimensiones → hechos → constraints.
# MAGIC Si se quisiera más paralelismo, `01_dm_dimensiones` se podría dividir en una tarea por dimensión (con un widget que indique cuál construir). `dm_dim_time` y los catálogos no dependen de nada y podrían arrancar al inicio del job, `dm_dim_zone` solo depende de `bronze_zones` y `dm_dim_date` de `silver_enriched`. `dm_fact_trips` esperaría a las seis. Con dimensiones tan pequeñas la ganancia es de segundos, así que se dejan en una sola tarea.
# MAGIC
# MAGIC **Dónde va la conciliación y qué pasa si falla:** al final de `dm_fact_trips`. Si no cuadra, el `raise` marca la tarea en rojo, `dm_constraints_vistas` **no corre** (no se declara `RELY` sobre datos sin validar) y el `on_failure` del job manda el correo. Las demás ramas (`gold_*`, ML) no se ven afectadas.
# MAGIC
# MAGIC **Las notebooks de análisis no van en el workflow:** `04_dm_preguntas_negocio` y `05_dm_rendimiento` son análisis puntuales, no parte de la carga diaria.
# MAGIC
# MAGIC **Mejora para producción (write–audit–publish):** las vistas son lógicas y `fact_trips` se sobrescribe en el mismo lugar. Si la conciliación falla, la tabla ya quedó escrita y las vistas de Finanzas la estarían leyendo. Para evitarlo:
# MAGIC 1. **Write:** escribir en una tabla de staging (`gold_taxi_dm_fact_trips_staging`).
# MAGIC 2. **Audit:** correr la conciliación contra el staging.
# MAGIC 3. **Publish:** solo si pasa, reemplazar `fact_trips` desde el staging. Otra opción es que, si la conciliación falla, se vuelva a la versión anterior con `RESTORE TABLE ... TO VERSION AS OF` (time travel de Delta).
# MAGIC
# MAGIC **Fragmento que se agregaría al YAML:**
# MAGIC
# MAGIC ```yaml
# MAGIC         # -------- Gold: data mart dimensional ------------------------------
# MAGIC         - task_key: dm_dimensiones
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: silver_enriched
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-data-mart/01_dm_dimensiones
# MAGIC
# MAGIC         - task_key: dm_fact_trips          # incluye la conciliación (falla si no cuadra)
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: dm_dimensiones
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-data-mart/02_dm_fact_trips
# MAGIC
# MAGIC         - task_key: dm_constraints_vistas
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: dm_fact_trips
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-data-mart/03_dm_constraints_vistas_permisos
# MAGIC ```