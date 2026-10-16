# Databricks notebook source
# MAGIC %run ../00-setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte 3: Constraints, vistas y permisos
# MAGIC
# MAGIC ### 3.1 Constraints
# MAGIC - **PRIMARY KEY** en cada dimensión, declarada con **`RELY`** (justificado más abajo). Antes se marca la columna como `NOT NULL`, que es requisito de la PK.
# MAGIC - **FOREIGN KEY** desde `fact_trips` hacia cada dimensión (7 FK; `dim_zone` recibe dos: pickup y dropoff).
# MAGIC - La celda es **re-ejecutable**: primero borra las constraints si existen (FK antes que PK, porque la FK depende de la PK) y luego las crea.

# COMMAND ----------

from pyspark.sql import functions as F

DIM_PKS = [
    (T_DM_DIM_DATE,         "date_key",         "pk_dm_dim_date"),
    (T_DM_DIM_TIME,         "hour_key",         "pk_dm_dim_time"),
    (T_DM_DIM_ZONE,         "zone_key",         "pk_dm_dim_zone"),
    (T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "pk_dm_dim_payment_type"),
    (T_DM_DIM_RATE_CODE,    "rate_code_key",    "pk_dm_dim_rate_code"),
    (T_DM_DIM_VENDOR,       "vendor_key",       "pk_dm_dim_vendor"),
]

FACT_FKS = [
    ("pickup_date_key",  T_DM_DIM_DATE,         "date_key",         "fk_dm_fact_pickup_date"),
    ("pickup_hour_key",  T_DM_DIM_TIME,         "hour_key",         "fk_dm_fact_pickup_hour"),
    ("pickup_zone_key",  T_DM_DIM_ZONE,         "zone_key",         "fk_dm_fact_pickup_zone"),
    ("dropoff_zone_key", T_DM_DIM_ZONE,         "zone_key",         "fk_dm_fact_dropoff_zone"),
    ("payment_type_key", T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "fk_dm_fact_payment_type"),
    ("rate_code_key",    T_DM_DIM_RATE_CODE,    "rate_code_key",    "fk_dm_fact_rate_code"),
    ("vendor_key",       T_DM_DIM_VENDOR,       "vendor_key",       "fk_dm_fact_vendor"),
]

# 1. Limpiar (re-ejecutable): FK primero, luego PK
for _, _, _, fk_name in FACT_FKS:
    spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} DROP CONSTRAINT IF EXISTS {fk_name}")
for table, _, pk_name in DIM_PKS:
    spark.sql(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {pk_name}")

# 2. PRIMARY KEY en cada dimensión (la columna debe ser NOT NULL)
for table, col, pk_name in DIM_PKS:
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")
    spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({col}) RELY")
    print(f"✓ PK  {pk_name:<26} {table.split('.')[-1]}({col})")

# 3. FOREIGN KEY de la tabla de hechos hacia cada dimensión
for fk_col, dim_table, dim_col, fk_name in FACT_FKS:
    spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} ALTER COLUMN {fk_col} SET NOT NULL")
    spark.sql(f"""
        ALTER TABLE {T_DM_FACT_TRIPS}
        ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk_col}) REFERENCES {dim_table} ({dim_col})
    """)
    print(f"✓ FK  {fk_name:<26} fact_trips({fk_col}) → {dim_table.split('.')[-1]}({dim_col})")

# COMMAND ----------

constraints = spark.sql(f"""
    SELECT tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name
    FROM `{CATALOG}`.information_schema.table_constraints tc
    JOIN `{CATALOG}`.information_schema.key_column_usage kcu
      ON  tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema  = kcu.constraint_schema
      AND tc.constraint_name    = kcu.constraint_name
    WHERE tc.table_schema = '{SCHEMA}'
      AND tc.table_name LIKE 'gold_taxi_dm_%'
    ORDER BY tc.constraint_type DESC, tc.table_name, tc.constraint_name
""")

n_pk = constraints.filter("constraint_type = 'PRIMARY KEY'").count()
n_fk = constraints.filter("constraint_type = 'FOREIGN KEY'").count()
assert n_pk == 6, f"Se esperaban 6 PK y hay {n_pk}"
assert n_fk == 7, f"Se esperaban 7 FK y hay {n_fk}"

print(f"✓ Registradas en information_schema: {n_pk} PRIMARY KEY y {n_fk} FOREIGN KEY")
display(constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Pregunta: si en Databricks las constraints son informativas (no se hacen cumplir al escribir), ¿para qué sirven? ¿Qué cambia con `RELY` y qué riesgo se asume?**
# MAGIC
# MAGIC **Para qué sirven aunque no se hagan cumplir:**
# MAGIC - **Documentan el modelo en el catálogo.** Catalog Explorer muestra qué columna es la llave de cada dimensión y cómo se relaciona la tabla de hechos con ellas (diagrama de entidad-relación). Quien llega nuevo entiende la estrella sin leer el código.
# MAGIC - **Las herramientas las aprovechan.** Herramientas de BI y de consulta en lenguaje natural (Power BI, Genie, etc.) leen las PK/FK para proponer los joins correctos automáticamente.
# MAGIC - **Declaran el contrato.** Dicen qué *debería* cumplirse. El que lo *hace* cumplir es la conciliación del notebook 02: si hay duplicados o huérfanas, el pipeline falla antes de llegar aquí.
# MAGIC
# MAGIC **Qué cambia con `RELY`:** se le dice al optimizador "confía en que esta PK es única". Con esa garantía puede reescribir consultas, por ejemplo:
# MAGIC - **Eliminar joins innecesarios:** si una consulta une `fact_trips` con `dim_zone` pero no usa ninguna columna de la dimensión, el join se puede quitar, porque una PK única no duplica ni pierde filas (con el left join a una PK única el resultado es el mismo).
# MAGIC - **Simplificar `DISTINCT` y agregaciones** sobre la llave, porque ya se sabe que no hay repetidos.
# MAGIC
# MAGIC **Riesgo que se asume:** Databricks **no verifica** que el dato cumpla la constraint. Si por un error una dimensión llega con una llave duplicada, el optimizador igual eliminaría o reescribiría el join como si fuera única, y las consultas devolverían **resultados incorrectos en silencio** (sin error), distintos a los que daría la misma consulta sin `RELY`. Por eso solo se declara `RELY` cuando hay un control que lo garantiza: aquí, la conciliación del notebook 02 valida cero duplicados en cada dimensión y falla el pipeline si no se cumple, y en el workflow las constraints se aplican después de esa conciliación.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 3.2 Vistas de consumo para Finanzas
# MAGIC Las vistas unen la estrella y exponen nombres legibles. Finanzas consulta **solo vistas**: no necesita saber de llaves, joins ni reglas de negocio.
# MAGIC
# MAGIC | Vista | Grano | Reglas de negocio incluidas |
# MAGIC |---|---|---|
# MAGIC | `gold_taxi_dm_v_revenue_mensual` | año × mes × borough de pickup × service_zone | Montos sumados como `DECIMAL` (exactos al centavo). `recargos_total` = `congestion_surcharge + airport_fee + cbd_congestion_fee` (la misma definición de la pregunta 6) |
# MAGIC | `gold_taxi_dm_v_propinas_tarjeta` | borough de pickup × franja horaria × fin de semana | Solo viajes con `tip_is_recorded` (tarjeta). Además de la propina promedio por viaje, expone la **tasa de propina** calculada desde sus componentes: `SUM(tip) / SUM(fare)` |
# MAGIC
# MAGIC Todas las columnas de montos son **aditivas**: Finanzas puede volver a sumarlas (por ejemplo, revenue anual = suma de los meses). Los ratios y promedios no se deben volver a promediar.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_REVENUE_MENSUAL}
COMMENT 'Revenue, propinas y recargos por mes y borough de pickup. Data mart Finanzas TLC.'
AS
SELECT
    d.year                                                    AS anio,
    d.month                                                   AS mes,
    d.month_name                                              AS nombre_mes,
    zp.borough                                                AS borough_pickup,
    zp.service_zone                                           AS service_zone_pickup,
    COUNT(*)                                                  AS viajes,
    SUM(CAST(f.total_amount         AS DECIMAL(18,2)))        AS revenue,
    SUM(CAST(f.fare_amount          AS DECIMAL(18,2)))        AS tarifa_base,
    SUM(CAST(f.tip_amount           AS DECIMAL(18,2)))        AS propinas,
    SUM(CAST(f.congestion_surcharge AS DECIMAL(18,2)))        AS congestion_surcharge,
    SUM(CAST(f.airport_fee          AS DECIMAL(18,2)))        AS airport_fee,
    SUM(CAST(f.cbd_congestion_fee   AS DECIMAL(18,2)))        AS cbd_congestion_fee,
    SUM(CAST(COALESCE(f.congestion_surcharge, 0)
           + COALESCE(f.airport_fee, 0)
           + COALESCE(f.cbd_congestion_fee, 0) AS DECIMAL(18,2))) AS recargos_total
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_DATE} d  ON f.pickup_date_key = d.date_key
JOIN {T_DM_DIM_ZONE} zp ON f.pickup_zone_key = zp.zone_key
GROUP BY d.year, d.month, d.month_name, zp.borough, zp.service_zone
""")
print(f"✓ {T_DM_V_REVENUE_MENSUAL}")

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_PROPINAS_TARJETA}
COMMENT 'Propina promedio con tarjeta (tip_is_recorded) por borough, franja horaria y fin de semana. Data mart Finanzas TLC.'
AS
SELECT
    zp.borough                                                AS borough_pickup,
    t.day_part                                                AS franja_horaria,
    d.is_weekend                                              AS es_fin_de_semana,
    COUNT(*)                                                  AS viajes,
    ROUND(AVG(f.tip_amount), 2)                               AS propina_promedio,
    ROUND(100 * SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0), 2) AS tasa_propina_pct,
    SUM(CAST(f.tip_amount AS DECIMAL(18,2)))                  AS propinas_total
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
JOIN {T_DM_DIM_DATE} d         ON f.pickup_date_key  = d.date_key
JOIN {T_DM_DIM_TIME} t         ON f.pickup_hour_key  = t.hour_key
JOIN {T_DM_DIM_ZONE} zp        ON f.pickup_zone_key  = zp.zone_key
WHERE p.tip_is_recorded
GROUP BY zp.borough, t.day_part, d.is_weekend
""")
print(f"✓ {T_DM_V_PROPINAS_TARJETA}")

# COMMAND ----------

vr = spark.table(T_DM_V_REVENUE_MENSUAL)
vp = spark.table(T_DM_V_PROPINAS_TARJETA)

# La vista de revenue debe cuadrar con la tabla de hechos (viajes y monto)
tot_vista = vr.agg(F.sum("viajes").alias("viajes"), F.sum("revenue").alias("revenue")).first()
tot_fact = spark.table(T_DM_FACT_TRIPS).agg(
    F.count("*").alias("viajes"),
    F.sum(F.col("total_amount").cast("decimal(38,2)")).alias("revenue"),
).first()
assert tot_vista["viajes"] == tot_fact["viajes"], "La vista de revenue perdió viajes"
assert tot_vista["revenue"] == tot_fact["revenue"], "El revenue de la vista no cuadra con fact_trips"
print(f"✓ v_revenue_mensual cuadra con fact_trips: {tot_vista['viajes']:,} viajes, ${tot_vista['revenue']:,}")

# La vista de propinas solo debe tener viajes con tarjeta
n_tarjeta = (
    spark.table(T_DM_FACT_TRIPS).join(spark.table(T_DM_DIM_PAYMENT_TYPE), "payment_type_key")
    .filter("tip_is_recorded").count()
)
assert vp.agg(F.sum("viajes")).first()[0] == n_tarjeta, "La vista de propinas no cuadra con los viajes con tarjeta"
print(f"✓ v_propinas_tarjeta cubre los {n_tarjeta:,} viajes con tarjeta")

display(vr.filter("borough_pickup = 'Manhattan'").orderBy("anio", "mes", "service_zone_pickup"))
display(vp.orderBy("borough_pickup", "franja_horaria", "es_fin_de_semana"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 3.3 Permisos
# MAGIC Finanzas (aquí, el compañero de pareja) debe poder leer **solo las vistas**, no `fact_trips` ni silver.
# MAGIC
# MAGIC | Permiso | Sobre | Para qué |
# MAGIC |---|---|---|
# MAGIC | `USE SCHEMA` | mi schema | Poder "entrar" al schema. No da acceso a ninguna tabla por sí solo |
# MAGIC | `SELECT` | `gold_taxi_dm_v_revenue_mensual` | Leer la vista |
# MAGIC | `SELECT` | `gold_taxi_dm_v_propinas_tarjeta` | Leer la vista |
# MAGIC
# MAGIC No se le da `SELECT` sobre `fact_trips`, las dimensiones ni silver. `USE CATALOG` sobre el catálogo del curso ya lo tiene cada estudiante.

# COMMAND ----------

COMPANERO = "correo.companero@upb.edu.co"   # ← correo del compañero de pareja (enmascarado en el repo)

spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{COMPANERO}`")
for vista in [T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA]:
    spark.sql(f"GRANT SELECT ON TABLE {vista} TO `{COMPANERO}`")

print(f"✓ Permisos otorgados a {COMPANERO}")

# COMMAND ----------

print("── Schema ──")
display(spark.sql(f"SHOW GRANTS `{COMPANERO}` ON SCHEMA `{CATALOG}`.`{SCHEMA}`"))

for obj in [T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA, T_DM_FACT_TRIPS, T_SILVER_TRIPS_ENRICHED]:
    print(f"── {obj.split('.')[-1]} ──")
    display(spark.sql(f"SHOW GRANTS `{COMPANERO}` ON TABLE {obj}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Evidencia:** `SHOW GRANTS` muestra `SELECT` para el compañero sobre `gold_taxi_dm_v_revenue_mensual` y `gold_taxi_dm_v_propinas_tarjeta`, (el `USE SCHEMA` sobre el schema se otorgó en la celda anterior). Sobre `gold_taxi_dm_fact_trips` y `silver_yellow_trips_enriched` no tiene `SELECT`, como confirman los errores de su prueba.
# MAGIC
# MAGIC Prueba del compañero en el SQL Editor con su usuario (capturas en `evidencia/` del repositorio):
# MAGIC - `SELECT * FROM ...gold_taxi_dm_v_revenue_mensual LIMIT 10` → ✅ 10 filas
# MAGIC - `SELECT * FROM ...gold_taxi_dm_v_propinas_tarjeta LIMIT 10` → ✅ 10 filas
# MAGIC - `SELECT * FROM ...gold_taxi_dm_fact_trips LIMIT 10` → ❌ `[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table '51810_databricks.sebastian_velasco.gold_taxi_dm_fact_trips'. SQLSTATE: 42501`
# MAGIC - `SELECT * FROM ...silver_yellow_trips_enriched LIMIT 10` → ❌ `[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table '51810_databricks.sebastian_velasco.silver_yellow_trips_enriched'. SQLSTATE: 42501`
# MAGIC
# MAGIC **Pregunta: ¿por qué el compañero lee la vista aunque no tenga `SELECT` sobre las tablas que la vista consulta?**
# MAGIC
# MAGIC Porque en Unity Catalog una vista se ejecuta con los permisos de su **dueño** sobre los objetos que consulta, no con los de quien la lee:
# MAGIC - Cuando el compañero consulta la vista, Unity Catalog verifica solo **sus** permisos sobre la vista: `USE CATALOG`, `USE SCHEMA` y `SELECT` sobre la vista.
# MAGIC - El acceso a `fact_trips` y a las dimensiones se verifica contra el **dueño de la vista** (yo), que es dueño de esas tablas.
# MAGIC - Si el compañero intenta consultar `fact_trips` directamente, se verifican sus propios permisos sobre la tabla, no los tiene, y recibe `INSUFFICIENT_PERMISSIONS` (SQLSTATE 42501).
# MAGIC
# MAGIC Por eso la vista funciona como una **capa de seguridad**: expone a Finanzas solo lo que necesita (agregados por mes, borough o franja, con nombres legibles), sin dar acceso al detalle viaje por viaje ni a silver. Una consecuencia: si el dueño perdiera el acceso a las tablas base, la vista dejaría de funcionar para todos los que la leen.
