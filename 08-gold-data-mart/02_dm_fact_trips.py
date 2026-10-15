# Databricks notebook source
# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(T_SILVER_TRIPS_ENRICHED)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Parte 2: Tabla de hechos `fact_trips`
# MAGIC **Grano:** un viaje. La tabla tiene exactamente las mismas filas que `silver_yellow_trips_enriched`.
# MAGIC
# MAGIC ### Diagnóstico de nulos
# MAGIC Antes de decidir NULL vs 0, se mide qué medidas llegan vacías, para qué viajes, y cuánto cambia el promedio con cada opción.

# COMMAND ----------

medidas_nulables = ["passenger_count", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]

# ¿Dónde aparecen los NULL? Por forma de pago y por período (antes/después de enero 2025)
diag = (
    silver
    .withColumn("es_flex_fare", F.col("payment_type") == 0)
    .withColumn("desde_2025", F.col("pickup_date") >= F.lit("2025-01-01"))
    .groupBy("es_flex_fare", "desde_2025")
    .agg(
        F.count("*").alias("viajes"),
        F.sum(F.col("RatecodeID").isNull().cast("int")).alias("null_RatecodeID"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in medidas_nulables],
    )
    .orderBy("es_flex_fare", "desde_2025")
)
display(diag)

# ¿Cómo cambia AVG(congestion_surcharge) con NULL vs 0?
display(silver.agg(
    F.round(F.avg("congestion_surcharge"), 4).alias("avg_con_NULL"),
    F.round(F.avg(F.coalesce("congestion_surcharge", F.lit(0.0))), 4).alias("avg_con_0"),
    F.count("congestion_surcharge").alias("viajes_con_dato"),
    F.count("*").alias("viajes_totales"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Decisión: las medidas sin dato quedan en **NULL**, no en 0
# MAGIC
# MAGIC | Caso | Columnas | Viajes |
# MAGIC |---|---|---|
# MAGIC | Flex Fare (`payment_type = 0`) | `passenger_count`, `congestion_surcharge`, `airport_fee` | 6.021.534 (15 %) |
# MAGIC | Antes de enero 2025 | `cbd_congestion_fee` | 18.836.580 |
# MAGIC
# MAGIC **Por qué NULL:** NULL significa "no se sabe" (la TLC no publica el dato, o el cargo no existía), y 0 significa "se sabe que fue cero". Son cosas distintas, y el mart no debe inventar un dato que la fuente no tiene.
# MAGIC
# MAGIC **Cómo cambia `AVG(congestion_surcharge)`:**
# MAGIC - Con **NULL**: `AVG` ignora los nulos y promedia solo los viajes con dato → **$2,3734** (33.668.412 viajes). Es el recargo promedio real de los viajes que lo reportan.
# MAGIC - Con **0**: `AVG` divide entre todos los viajes → **$2,0133**. Subestima el recargo un 15 %, porque trata 6 millones de viajes Flex Fare (que en su mayoría sí lo pagaron) como si hubieran pagado $0.
# MAGIC - `SUM` da lo mismo en los dos casos: `SUM` ignora los NULL y sumar ceros no cambia nada. La decisión solo afecta promedios y conteos.
# MAGIC
# MAGIC **Consecuencias:**
# MAGIC - Quien necesite "recargo promedio sobre todos los viajes" lo hace de forma explícita: `AVG(COALESCE(congestion_surcharge, 0))`.
# MAGIC - Al sumar recargos dentro de una fila (`congestion_surcharge + airport_fee + cbd_congestion_fee`), cualquier NULL vuelve NULL el resultado. En la Parte 4 se suman por separado con `SUM` de cada columna, o con `COALESCE` dentro de la fila.
# MAGIC
# MAGIC Las **llaves** son otra cosa: nunca quedan en NULL. Toda llave sin valor, o sin pareja en su dimensión, apunta al miembro `-1`.

# COMMAND ----------

def _fk(df, fk_name, src_expr, dim_table, dim_key):
    """Left join con la dimensión (broadcast) y coalesce a -1.
    Si el código es NULL o no existe en la dimensión, la llave queda en -1."""
    dim_keys = spark.table(dim_table).select(F.col(dim_key).alias("_k"))
    return (
        df.join(F.broadcast(dim_keys), src_expr.cast("int") == F.col("_k"), "left")
          .withColumn(fk_name, F.coalesce(F.col("_k"), F.lit(-1)))
          .drop("_k")
    )

fact = silver
fact = _fk(fact, "pickup_date_key",  F.date_format("pickup_date", "yyyyMMdd"), T_DM_DIM_DATE,         "date_key")
fact = _fk(fact, "pickup_hour_key",  F.col("pickup_hour"),                     T_DM_DIM_TIME,         "hour_key")
fact = _fk(fact, "pickup_zone_key",  F.col("PULocationID"),                    T_DM_DIM_ZONE,         "zone_key")
fact = _fk(fact, "dropoff_zone_key", F.col("DOLocationID"),                    T_DM_DIM_ZONE,         "zone_key")
fact = _fk(fact, "payment_type_key", F.col("payment_type"),                    T_DM_DIM_PAYMENT_TYPE, "payment_type_key")
fact = _fk(fact, "rate_code_key",    F.col("RatecodeID"),                      T_DM_DIM_RATE_CODE,    "rate_code_key")
fact = _fk(fact, "vendor_key",       F.col("VendorID"),                        T_DM_DIM_VENDOR,       "vendor_key")

FK_COLS = ["pickup_date_key", "pickup_hour_key", "pickup_zone_key", "dropoff_zone_key",
           "payment_type_key", "rate_code_key", "vendor_key"]

MEASURE_COLS = ["passenger_count", "trip_distance", "trip_duration_min",
                "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                "improvement_surcharge", "congestion_surcharge", "airport_fee",
                "cbd_congestion_fee", "total_amount"]

fact_trips = fact.select(
    *FK_COLS,
    F.col("passenger_count").cast("int").alias("passenger_count"),
    *[c for c in MEASURE_COLS if c != "passenger_count"],
    "tpep_pickup_datetime", "tpep_dropoff_datetime",   # trazabilidad
)

(
    fact_trips.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("pickup_date_key", "pickup_zone_key")
    .saveAsTable(T_DM_FACT_TRIPS))
)
print(f"✓ {T_DM_FACT_TRIPS}")

# COMMAND ----------

ft = spark.table(T_DM_FACT_TRIPS)
print(f"Filas en fact_trips: {ft.count():,}")
ft.printSchema()

# ¿Cuántos viajes quedaron apuntando al miembro -1 en cada llave?
display(ft.select([F.sum((F.col(c) == -1).cast("int")).alias(c) for c in FK_COLS]))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Conciliación
# MAGIC Esta celda **falla el notebook** (`raise`) si algo no cuadra:
# MAGIC 1. `COUNT(*)` de `fact_trips` = `COUNT(*)` de silver
# MAGIC 2. `SUM(total_amount)` de `fact_trips` = `SUM(total_amount)` de silver. Se suma como `DECIMAL(38,2)` para que la comparación sea exacta: los montos tienen 2 decimales, y sumar `DOUBLE` en distinto orden (Spark reparte las filas entre tareas) puede diferir en fracciones de centavo.
# MAGIC 3. Cero llaves huérfanas: cada llave foránea existe en su dimensión (`left_anti` join)
# MAGIC 4. Cero llaves duplicadas en cada dimensión
# MAGIC
# MAGIC Se acumulan todos los errores y se reportan juntos, para ver en una sola corrida todo lo que falló.

# COMMAND ----------

silver_rc = spark.table(T_SILVER_TRIPS_ENRICHED)
fact_rc   = spark.table(T_DM_FACT_TRIPS)

errores = []

# 1 y 2. Filas y monto total
def _resumen(df):
    return df.agg(
        F.count("*").alias("filas"),
        F.sum(F.col("total_amount").cast("decimal(38,2)")).alias("total_amount"),
    ).first()

s, f = _resumen(silver_rc), _resumen(fact_rc)
print(f"Filas         silver={s['filas']:,}   fact={f['filas']:,}")
print(f"total_amount  silver={s['total_amount']:,}   fact={f['total_amount']:,}")

if s["filas"] != f["filas"]:
    errores.append(f"COUNT(*) no cuadra: silver={s['filas']:,} fact={f['filas']:,}")
if s["total_amount"] != f["total_amount"]:
    errores.append(f"SUM(total_amount) no cuadra: silver={s['total_amount']} fact={f['total_amount']}")

# 3. Llaves huérfanas (FK de la tabla de hechos sin fila en su dimensión)
FK_MAP = [
    ("pickup_date_key",  T_DM_DIM_DATE,         "date_key"),
    ("pickup_hour_key",  T_DM_DIM_TIME,         "hour_key"),
    ("pickup_zone_key",  T_DM_DIM_ZONE,         "zone_key"),
    ("dropoff_zone_key", T_DM_DIM_ZONE,         "zone_key"),
    ("payment_type_key", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
    ("rate_code_key",    T_DM_DIM_RATE_CODE,    "rate_code_key"),
    ("vendor_key",       T_DM_DIM_VENDOR,       "vendor_key"),
]

for fk, dim_table, pk in FK_MAP:
    dim_keys = spark.table(dim_table).select(F.col(pk).alias(fk))
    huerfanas = fact_rc.select(fk).join(F.broadcast(dim_keys), on=fk, how="left_anti").count()
    print(f"Huérfanas {fk:<18} → {huerfanas:,}")
    if huerfanas:
        errores.append(f"{huerfanas:,} llaves huérfanas en {fk} (no existen en {dim_table})")

# 4. Llaves duplicadas en cada dimensión
DIMS = sorted({(t, pk) for _, t, pk in FK_MAP})
for dim_table, pk in DIMS:
    dups = spark.table(dim_table).groupBy(pk).count().filter("count > 1").count()
    print(f"Duplicadas {dim_table.split('.')[-1]:<32} → {dups}")
    if dups:
        errores.append(f"{dups} llaves duplicadas en {dim_table}.{pk}")

# Resultado: si hay cualquier error, el notebook FALLA
if errores:
    raise AssertionError("❌ Conciliación fallida:\n  - " + "\n  - ".join(errores))

print("\n✓ Conciliación OK: filas, montos, llaves foráneas y dimensiones cuadran")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Pregunta: ¿por qué la conciliación debe hacer fallar el notebook en lugar de solo mostrar los números?**
# MAGIC
# MAGIC Porque a las 3 a. m. nadie está mirando la salida. Dentro de un workflow, un notebook que imprime "silver = 39.689.946, fact = 39.102.311" y **termina bien** queda marcado como exitoso:
# MAGIC - **Las tareas siguientes corren igual.** Las vistas, los permisos y los dashboards se construyen sobre datos incompletos, y el error se propaga por toda la capa Gold.
# MAGIC - **Nadie se entera.** El job queda en verde, no salta ninguna alerta, y el problema lo descubre Finanzas días después, cuando un reporte no cuadra con la contabilidad. A esas alturas cuesta mucho más rastrearlo.
# MAGIC - **Un `raise` vuelve el problema visible y lo contiene.** La tarea queda en rojo, el workflow detiene las tareas dependientes (no publica datos malos), se dispara la notificación de falla (correo, Slack) y el mensaje dice exactamente qué no cuadró. Los consumidores siguen viendo la última versión buena.
# MAGIC
# MAGIC Un chequeo que solo imprime es documentación. Un chequeo que falla es un **control de calidad**: convierte una suposición ("el join no perdió filas") en una garantía que el orquestador hace cumplir.