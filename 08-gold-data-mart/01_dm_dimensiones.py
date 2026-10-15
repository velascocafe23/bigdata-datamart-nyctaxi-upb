# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Data mart dimensional — Finanzas TLC (capa Gold)
# MAGIC
# MAGIC ## Parte 0: Diseño
# MAGIC
# MAGIC ### 1. Grano de la tabla de hechos
# MAGIC **Cada fila de `fact_trips` es un viaje de taxi amarillo que pasó las reglas de calidad de silver.**
# MAGIC La tabla tiene exactamente las mismas filas que `silver_yellow_trips_enriched` (39.689.946 viajes, julio 2024 – junio 2025).
# MAGIC
# MAGIC **¿Por qué no un grano agregado (p. ej. "cada fila es una zona en un día")?**
# MAGIC Al agregar se pierde para siempre todo atributo que no quedó en el `GROUP BY`. Con "zona × día" ya no se podrían responder:
# MAGIC - **P2** (ticket promedio de viajes que *terminan* en aeropuerto): se pierde la zona de destino.
# MAGIC - **P3** (viajes con tarifa JFK que empiezan *o* terminan en JFK): se pierden `RatecodeID` y la zona de destino.
# MAGIC - **P4** (propina por `day_part` y fin de semana): se pierde la hora y la forma de pago (y con ella `tip_is_recorded`).
# MAGIC - **P5** (mezcla de formas de pago por mes): se pierde `payment_type`.
# MAGIC - **Parte 5** (top 20 pares origen → destino): se pierde el par de zonas.
# MAGIC
# MAGIC Además, un agregado guarda promedios ya calculados, y promediar promedios da resultados incorrectos. El grano atómico permite responder cualquier combinación de dimensiones; los agregados se derivan de él, nunca al revés.
# MAGIC
# MAGIC ### 2. Diagrama del modelo estrella
# MAGIC
# MAGIC ```
# MAGIC                   ┌────────────────────────────────────────┐
# MAGIC                   │               fact_trips               │
# MAGIC                   │        grano: 1 fila = 1 viaje         │
# MAGIC                   ├────────────────────────────────────────┤
# MAGIC [ dim_date ] ◄────┤ pickup_date_key      payment_type_key  ├────► [ dim_payment_type ]
# MAGIC [ dim_time ] ◄────┤ pickup_hour_key      rate_code_key     ├────► [ dim_rate_code ]
# MAGIC               ┌───┤ pickup_zone_key      vendor_key        ├────► [ dim_vendor ]
# MAGIC [ dim_zone ] ◄┤   │                                        │
# MAGIC               └───┤ dropoff_zone_key                       │
# MAGIC                   ├────────────────────────────────────────┤
# MAGIC                   │ medidas (13):                          │
# MAGIC                   │ fare_amount, extra, mta_tax,           │
# MAGIC                   │ tip_amount, tolls_amount,              │
# MAGIC                   │ improvement_surcharge,                 │
# MAGIC                   │ congestion_surcharge, airport_fee,     │
# MAGIC                   │ cbd_congestion_fee, total_amount,      │
# MAGIC                   │ passenger_count, trip_distance,        │
# MAGIC                   │ trip_duration_min                      │
# MAGIC                   └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC `dim_zone` es una **role-playing dimension**: una sola tabla física que la tabla de hechos referencia dos veces, como zona de origen (`pickup_zone_key`) y como zona de destino (`dropoff_zone_key`). Las flechas van de la llave foránea en `fact_trips` a la llave primaria de cada dimensión.
# MAGIC
# MAGIC ### 3. Dimensiones
# MAGIC
# MAGIC | Dimensión | Llave | Fuente | Atributos |
# MAGIC |---|---|---|---|
# MAGIC | `dim_date` | `date_key` INT `yyyyMMdd` | Generada con `sequence` + `explode` entre la fecha mín. y máx. de silver; festivos federales de EE. UU. (OPM) | `full_date`, `year`, `quarter`, `month`, `month_name`, `day_of_month`, `day_of_week` (1 = domingo … 7 = sábado), `day_name`, `is_weekend`, `is_holiday`, `holiday_name` |
# MAGIC | `dim_time` | `hour_key` 0–23 | Generada (24 filas) | `hour_label` (07:00–07:59), `day_part` (madrugada/mañana/tarde/noche), `is_rush_hour` (7–9 h y 17–19 h, igual que silver) |
# MAGIC | `dim_zone` | `zone_key` = `LocationID` | `bronze_taxi_zones` | `borough`, `zone`, `service_zone`, `is_airport` (Newark, JFK, LaGuardia), `is_special_zone` (264 Unknown, 265 Outside of NYC) + miembro `-1` |
# MAGIC | `dim_payment_type` | `payment_type_key` 0–6 | Data dictionary TLC | `payment_type_desc`, `tip_is_recorded` (true solo para tarjeta) + miembro `-1` |
# MAGIC | `dim_rate_code` | `rate_code_key` 1–6 | Data dictionary TLC | `rate_code_desc` + miembro `-1` (Flex Fare llega sin `RatecodeID`) |
# MAGIC | `dim_vendor` | `vendor_key` 1–2 | Data dictionary TLC | `vendor_name` + miembro `-1` |
# MAGIC
# MAGIC ### 4. Medidas y su aditividad
# MAGIC
# MAGIC | Medida | Tipo | Comentario |
# MAGIC |---|---|---|
# MAGIC | `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_amount` | **Aditiva** | Montos en dólares: se suman por cualquier dimensión |
# MAGIC | `trip_distance`, `trip_duration_min` | **Aditiva** | Millas y minutos totales recorridos |
# MAGIC | `passenger_count` | **Aditiva** | Pasajeros transportados (NULL en Flex Fare) |
# MAGIC | Conteo de viajes (`COUNT(*)`) | **Aditiva** | No es columna: sale del grano |
# MAGIC | — | **Semi-aditiva** | No hay en este mart. Las semi-aditivas son saldos o fotos en un instante (inventario, taxis activos a una hora): se suman entre zonas pero no a lo largo del tiempo. Un hecho transaccional como un viaje no tiene |
# MAGIC | `tip_rate`, `avg_speed_mph`, `cost_per_mile` | **No aditivas** | Ratios. **No se guardan** en la tabla de hechos; se recalculan en las consultas desde sus componentes aditivos |
# MAGIC
# MAGIC ### 5. ¿`tip_rate` y `avg_speed_mph` deben ir como medidas en la tabla de hechos?
# MAGIC **No.** Son ratios por viaje, y un ratio no se puede sumar ni promediar directamente:
# MAGIC - **`SUM(tip_rate)`** no significa nada: sumar porcentajes de viajes distintos da un número sin unidad.
# MAGIC - **`AVG(tip_rate)`** le da el mismo peso a un viaje de $5 que a uno de $90. Ejemplo: viaje A con tarifa $10 y propina $3 (30 %), viaje B con tarifa $90 y propina $9 (10 %). `AVG(tip_rate)` = 20 %, pero la tasa real de Finanzas es `SUM(tip)/SUM(fare)` = 12 / 100 = **12 %**.
# MAGIC - Lo mismo con la velocidad: `AVG(avg_speed_mph)` sobrepondera los viajes cortos. La velocidad correcta de un conjunto de viajes es `SUM(trip_distance) / (SUM(trip_duration_min)/60)`.
# MAGIC
# MAGIC Además, `tip_rate` en silver incluye efectivo y Flex Fare con propina 0 (no registrada), lo que baja el promedio. La regla correcta (propina solo donde `tip_is_recorded`) vive en `dim_payment_type`.
# MAGIC
# MAGIC **Decisión:** la tabla de hechos guarda solo los componentes aditivos (`tip_amount`, `fare_amount`, `trip_distance`, `trip_duration_min`), y los ratios se calculan en las vistas como `SUM(numerador) / SUM(denominador)`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(T_SILVER_TRIPS_ENRICHED)
print(T_DM_DIM_DATE)
print(f"Viajes en silver: {silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Parte 1: Dimensiones
# MAGIC
# MAGIC ### 1.1 `dim_date`
# MAGIC Una fila por día entre la fecha mínima y la máxima de silver, generada con `sequence` + `explode` (no con `DISTINCT pickup_date`).
# MAGIC
# MAGIC **Festivos:** los 11 festivos federales de EE. UU. que define 5 U.S.C. § 6103, según el calendario de la Office of Personnel Management (OPM): https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/
# MAGIC Se calculan por regla para cualquier año: fecha fija o "n-ésimo lunes/jueves del mes". Si un festivo de fecha fija cae sábado, se observa el viernes anterior; si cae domingo, el lunes siguiente (regla de OPM). Así la dimensión sigue siendo correcta si después se cargan más meses.

# COMMAND ----------

from datetime import date, timedelta

def _nth_weekday(year, month, weekday, n):
    """n-ésimo día de la semana del mes (weekday: 0 = lunes … 6 = domingo). n = -1 es el último."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    d = (date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)

def _observed(d):
    """Regla OPM: sábado → viernes anterior, domingo → lunes siguiente."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d

def us_federal_holidays(year):
    return [
        (_observed(date(year, 1, 1)),   "New Year's Day"),
        (_nth_weekday(year, 1, 0, 3),   "Birthday of Martin Luther King, Jr."),
        (_nth_weekday(year, 2, 0, 3),   "Washington's Birthday"),
        (_nth_weekday(year, 5, 0, -1),  "Memorial Day"),
        (_observed(date(year, 6, 19)),  "Juneteenth National Independence Day"),
        (_observed(date(year, 7, 4)),   "Independence Day"),
        (_nth_weekday(year, 9, 0, 1),   "Labor Day"),
        (_nth_weekday(year, 10, 0, 2),  "Columbus Day"),
        (_observed(date(year, 11, 11)), "Veterans Day"),
        (_nth_weekday(year, 11, 3, 4),  "Thanksgiving Day"),
        (_observed(date(year, 12, 25)), "Christmas Day"),
    ]

# COMMAND ----------

rango = silver.agg(F.min("pickup_date").alias("d_min"), F.max("pickup_date").alias("d_max")).first()
d_min, d_max = rango["d_min"], rango["d_max"]
print(f"Rango de silver: {d_min} → {d_max}")

festivos = [
    (d, nombre)
    for y in range(d_min.year, d_max.year + 1)
    for d, nombre in us_federal_holidays(y)
    if d_min <= d <= d_max
]
holidays_df = spark.createDataFrame(festivos, "full_date DATE, holiday_name STRING")

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
DIAS = ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"]  # 1 = domingo, igual que silver

dim_date = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(d_min), F.lit(d_max))).alias("full_date"))
    .withColumn("date_key",     F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("full_date"))
    .withColumn("quarter",      F.quarter("full_date"))
    .withColumn("month",        F.month("full_date"))
    .withColumn("month_name",   F.element_at(F.array(*[F.lit(m) for m in MESES]), F.col("month")))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week",  F.dayofweek("full_date"))
    .withColumn("day_name",     F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.col("day_of_week")))
    .withColumn("is_weekend",   F.col("day_of_week").isin(1, 7))
    .join(F.broadcast(holidays_df), on="full_date", how="left")
    .withColumn("is_holiday",   F.col("holiday_name").isNotNull())
    .select("date_key", "full_date", "year", "quarter", "month", "month_name",
            "day_of_month", "day_of_week", "day_name", "is_weekend", "is_holiday", "holiday_name")
)

(
    dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_DM_DIM_DATE)
)
print(f"✓ {T_DM_DIM_DATE}")

# COMMAND ----------

dd = spark.table(T_DM_DIM_DATE)
n_esperado = (d_max - d_min).days + 1
n = dd.count()

assert n == n_esperado, f"Se esperaban {n_esperado} días y hay {n}"
assert dd.select("date_key").distinct().count() == n, "Hay date_key duplicadas"

# Misma convención de día de la semana que silver (1 = domingo … 7 = sábado)
desfase = (
    silver.select("pickup_date", "pickup_dayofweek").distinct()
    .join(dd, silver["pickup_date"] == dd["full_date"])
    .filter(F.col("pickup_dayofweek") != F.col("day_of_week"))
    .count()
)
assert desfase == 0, f"{desfase} fechas con día de la semana distinto a silver"

print(f"✓ {n} días, sin duplicados, day_of_week coincide con silver")
display(dd.filter("is_holiday").orderBy("full_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Pregunta: ¿qué se pierde si `dim_date` sale de los viajes y un día no tuvo ninguno?**
# MAGIC
# MAGIC Ese día desaparece del calendario. `dim_date` describe el calendario, no la actividad; si se deriva de los hechos, hereda sus huecos:
# MAGIC - **Series con huecos:** en "viajes por día" el día no aparece con 0, sino que no aparece. Un gráfico une el día anterior con el siguiente y oculta la caída (por ejemplo, una tormenta o una falla del sistema de la TLC que no reportó viajes).
# MAGIC - **Promedios inflados:** "viajes promedio por día" divide por los días *con* viajes, no por los días del período.
# MAGIC - **Comparaciones de tiempo rotas:** las ventanas móviles, el año contra año o "mismo día de la semana anterior" asumen una fila por día; con huecos comparan contra el día equivocado.
# MAGIC - **Se pierden atributos del día:** si el día sin viajes era festivo, desaparece de cualquier análisis de festivos, y los filtros de BI por fecha no lo muestran.
# MAGIC - **Dependencia de la carga:** la dimensión cambiaría con cada carga de hechos, y un viaje que llegue tarde para una fecha nueva no tendría fila en la dimensión (llave huérfana).

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 1.2 `dim_time`
# MAGIC 24 filas, una por hora del día. `is_rush_hour` usa la misma definición que silver (7–9 h y 17–19 h), y se verifica contra silver más abajo.
# MAGIC
# MAGIC | `day_part` | Horas |
# MAGIC |---|---|
# MAGIC | madrugada | 0–5 |
# MAGIC | mañana | 6–11 |
# MAGIC | tarde | 12–17 |
# MAGIC | noche | 18–23 |

# COMMAND ----------

dim_time = (
    spark.range(24).select(F.col("id").cast("int").alias("hour_key"))
    .withColumn("hour_label", F.format_string("%02d:00–%02d:59", F.col("hour_key"), F.col("hour_key")))
    .withColumn("day_part",
        F.when(F.col("hour_key") <= 5,  "madrugada")
         .when(F.col("hour_key") <= 11, "mañana")
         .when(F.col("hour_key") <= 17, "tarde")
         .otherwise("noche"))
    .withColumn("is_rush_hour",
        F.col("hour_key").between(7, 9) | F.col("hour_key").between(17, 19))
)

(
    dim_time.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_DM_DIM_TIME)
)
print(f"✓ {T_DM_DIM_TIME}")

# COMMAND ----------

dt = spark.table(T_DM_DIM_TIME)

assert dt.count() == 24, "dim_time debe tener 24 filas"
assert dt.select("hour_key").distinct().count() == 24, "Hay hour_key duplicadas"

# is_rush_hour debe coincidir con silver para cada hora
discrepancias = (
    silver.select("pickup_hour", "is_rush_hour").distinct()
    .join(dt, silver["pickup_hour"] == dt["hour_key"])
    .filter(silver["is_rush_hour"] != dt["is_rush_hour"])
)
n_disc = discrepancias.count()
assert n_disc == 0, f"{n_disc} horas con is_rush_hour distinto a silver"

print("✓ 24 horas, sin duplicados, is_rush_hour coincide con silver")
display(dt.orderBy("hour_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 1.3 `dim_zone`
# MAGIC Fuente: `bronze_taxi_zones` (265 zonas de la TLC). `zone_key` = `LocationID`.
# MAGIC
# MAGIC - **`is_airport`**: true para `Newark Airport` (1), `JFK Airport` (132) y `LaGuardia Airport` (138), con los nombres exactos del lookup. No se usa `service_zone = 'Airports'` porque Newark no queda incluido: su `service_zone` es `EWR`.
# MAGIC - **`is_special_zone`**: true para 264 (`Unknown`) y 265 (`Outside of NYC`). No se borran, porque son viajes reales y la tabla de hechos debe conservar todas las filas de silver. También se marca así el miembro técnico `-1`, para que un filtro `NOT is_special_zone` excluya todo lo que no es una zona real.
# MAGIC - **Miembro `-1`** (Desconocido / No aplica): por si alguna llave de zona llegara sin valor.
# MAGIC - Es una **role-playing dimension**: una sola tabla física que `fact_trips` usa como `pickup_zone_key` y como `dropoff_zone_key`.

# COMMAND ----------

AIRPORT_ZONES = ["Newark Airport", "JFK Airport", "LaGuardia Airport"]
SPECIAL_ZONE_IDS = [264, 265]
UNKNOWN = "Desconocido / No aplica"

zones_src = spark.table(T_BRONZE_ZONES)

dim_zone_real = zones_src.select(
    F.col("LocationID").cast("int").alias("zone_key"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone"),
    F.col("service_zone"),
    F.col("Zone").isin(AIRPORT_ZONES).alias("is_airport"),
    F.col("LocationID").isin(SPECIAL_ZONE_IDS).alias("is_special_zone"),
)

miembro_desconocido = spark.createDataFrame(
    [(-1, UNKNOWN, UNKNOWN, UNKNOWN, False, True)],
    dim_zone_real.schema,
)

dim_zone = dim_zone_real.unionByName(miembro_desconocido)

(
    dim_zone.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_DM_DIM_ZONE)
)
print(f"✓ {T_DM_DIM_ZONE}")

# COMMAND ----------

dz = spark.table(T_DM_DIM_ZONE)

assert dz.count() == 266, f"Se esperaban 266 filas (265 + miembro -1), hay {dz.count()}"
assert dz.select("zone_key").distinct().count() == dz.count(), "Hay zone_key duplicadas"
assert dz.filter("is_airport").count() == 3, "Deben ser exactamente 3 aeropuertos: revisa los nombres"

# Toda zona de silver (origen y destino) debe existir en la dimensión
zonas_silver = (
    silver.select(F.col("PULocationID").alias("zone_key"))
    .union(silver.select(F.col("DOLocationID").alias("zone_key")))
    .distinct()
)
faltantes = zonas_silver.join(dz, on="zone_key", how="left_anti").count()
assert faltantes == 0, f"{faltantes} zonas de silver no están en dim_zone"

print("✓ 266 filas, sin duplicados, 3 aeropuertos, todas las zonas de silver cubiertas")
display(dz.filter("is_airport OR is_special_zone").orderBy("zone_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 1.4 `dim_payment_type`, `dim_rate_code`, `dim_vendor`
# MAGIC Catálogos construidos a partir del **TLC Yellow Taxi Trip Records Data Dictionary** (versión del 18 de marzo de 2025): https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
# MAGIC
# MAGIC - Cubren los códigos que silver acepta: `payment_type` 0–6, `RatecodeID` 1–6, `VendorID` 1 y 2. El diccionario también trae `VendorID` 6 (Myle) y 7 (Helix) y `RatecodeID` 99 (Null/unknown), pero silver rechaza esos viajes (`valid_vendor`, `valid_rate_code`), así que nunca llegan al mart.
# MAGIC - Se guarda la descripción oficial en inglés y una en español para Finanzas.
# MAGIC - **`dim_payment_type.tip_is_recorded`**: true solo para tarjeta (1). En efectivo y Flex Fare la propina queda en 0 aunque el pasajero la haya dado, así que la regla "propina solo con tarjeta" vive aquí, en un solo lugar, y no repetida en cada consulta.
# MAGIC - **`dim_payment_type.payment_group`**: agrupa para Finanzas (Tarjeta, Efectivo, Flex Fare, Otros).
# MAGIC - **`dim_rate_code.is_airport_rate`**: true para las tarifas JFK (2) y Newark (3).
# MAGIC - Cada catálogo incluye el **miembro `-1` = Desconocido / No aplica** (por ejemplo, los viajes Flex Fare llegan sin `RatecodeID`).

# COMMAND ----------

UNKNOWN = "Desconocido / No aplica"

def _write_dim(rows, schema, table):
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✓ {table} ({df.count()} filas)")

# ── Forma de pago ────────────────────────────────────────────────────────────
_write_dim(
    [
        (-1, UNKNOWN,          UNKNOWN,                 "Otros",     False),
        (0,  "Flex Fare trip", "Viaje Flex Fare",       "Flex Fare", False),
        (1,  "Credit card",    "Tarjeta de crédito",    "Tarjeta",   True),
        (2,  "Cash",           "Efectivo",              "Efectivo",  False),
        (3,  "No charge",      "Sin cargo",             "Otros",     False),
        (4,  "Dispute",        "En disputa",            "Otros",     False),
        (5,  "Unknown",        "Desconocido",           "Otros",     False),
        (6,  "Voided trip",    "Viaje anulado",         "Otros",     False),
    ],
    "payment_type_key INT, payment_type_desc STRING, payment_type_desc_es STRING, "
    "payment_group STRING, tip_is_recorded BOOLEAN",
    T_DM_DIM_PAYMENT_TYPE,
)

# ── Tipo de tarifa ───────────────────────────────────────────────────────────
_write_dim(
    [
        (-1, UNKNOWN,                  UNKNOWN,                   False),
        (1,  "Standard rate",          "Tarifa estándar",         False),
        (2,  "JFK",                    "Tarifa fija JFK",         True),
        (3,  "Newark",                 "Tarifa Newark",           True),
        (4,  "Nassau or Westchester",  "Nassau o Westchester",    False),
        (5,  "Negotiated fare",        "Tarifa negociada",        False),
        (6,  "Group ride",             "Viaje compartido",        False),
    ],
    "rate_code_key INT, rate_code_desc STRING, rate_code_desc_es STRING, is_airport_rate BOOLEAN",
    T_DM_DIM_RATE_CODE,
)

# ── Proveedor ────────────────────────────────────────────────────────────────
_write_dim(
    [
        (-1, UNKNOWN),
        (1,  "Creative Mobile Technologies, LLC"),
        (2,  "Curb Mobility, LLC"),
    ],
    "vendor_key INT, vendor_name STRING",
    T_DM_DIM_VENDOR,
)

# COMMAND ----------

checks = [
    (T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "payment_type"),
    (T_DM_DIM_RATE_CODE,    "rate_code_key",    "RatecodeID"),
    (T_DM_DIM_VENDOR,       "vendor_key",       "VendorID"),
]

for table, key, src_col in checks:
    dim = spark.table(table)
    assert dim.select(key).distinct().count() == dim.count(), f"{table}: llaves duplicadas"
    assert dim.filter(F.col(key) == -1).count() == 1, f"{table}: falta el miembro -1"

    # Códigos de silver (NULL → -1, igual que hará la tabla de hechos)
    codigos = silver.select(F.coalesce(F.col(src_col).cast("int"), F.lit(-1)).alias(key)).distinct()
    faltantes = codigos.join(dim, on=key, how="left_anti")
    n_falt = faltantes.count()
    assert n_falt == 0, f"{table}: códigos de silver sin fila en la dimensión: {[r[0] for r in faltantes.collect()]}"
    print(f"✓ {table}: {dim.count()} filas, sin duplicados, cubre todos los códigos de silver")

display(spark.table(T_DM_DIM_PAYMENT_TYPE).orderBy("payment_type_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Pregunta: ¿por qué un miembro `-1` es mejor que dejar la llave foránea en NULL?**
# MAGIC
# MAGIC - **Los joins no pierden filas.** En SQL `NULL = NULL` no es verdadero, así que un `INNER JOIN` con la dimensión descarta en silencio los viajes con llave NULL. Con `-1` siempre hay pareja: `COUNT(*)` y `SUM(total_amount)` del mart cuadran con silver, sin importar quién escriba la consulta.
# MAGIC - **El "desconocido" se ve y tiene nombre.** En un reporte, un grupo NULL aparece en blanco o como `null`, y nadie sabe si es un error. `-1` aparece como "Desconocido / No aplica": Finanzas ve cuántos viajes Flex Fare no tienen tipo de tarifa y lo puede filtrar a propósito.
# MAGIC - **El miembro puede tener atributos.** Por ejemplo, `tip_is_recorded = false` o `is_special_zone = true`, así los filtros de negocio funcionan sin casos especiales (`OR key IS NULL`).
# MAGIC - **Integridad verificable.** Con `-1` la regla es simple: "toda llave foránea existe en su dimensión", y se comprueba con un `left_anti` join en la conciliación. Con NULL habría que distinguir entre "NULL permitido" y "llave rota".
# MAGIC - **Menos trampas de SQL.** `COUNT(col)`, `NOT IN` y las comparaciones se comportan distinto con NULL. Una llave entera sin NULL evita esos errores.