# Databricks notebook source
# MAGIC %run ../00-setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # Parte 4: Preguntas de negocio
# MAGIC Todas las consultas usan **solo el mart** (vistas o joins sobre la estrella). Ninguna toca silver.
# MAGIC Los ratios se calculan siempre desde sus componentes aditivos (`SUM / SUM`), nunca promediando ratios.
# MAGIC
# MAGIC ## P1. Revenue mensual por borough de pickup y service_zone. ¿Qué participación tienen los aeropuertos?

# COMMAND ----------

display(spark.sql(f"""
    SELECT anio, mes, nombre_mes, borough_pickup, service_zone_pickup,
           viajes, revenue,
           ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY anio, mes), 2) AS pct_revenue_del_mes
    FROM {T_DM_V_REVENUE_MENSUAL}
    ORDER BY anio, mes, revenue DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                                         AS viajes_total,
        ROUND(SUM(f.total_amount), 2)                                    AS revenue_total,

        -- Viajes que EMPIEZAN en un aeropuerto (mismo criterio que la vista: borough de pickup)
        ROUND(100 * SUM(CASE WHEN zp.is_airport THEN 1 ELSE 0 END) / COUNT(*), 2)                       AS pct_viajes_pickup_aeropuerto,
        ROUND(100 * SUM(CASE WHEN zp.is_airport THEN f.total_amount END) / SUM(f.total_amount), 2)      AS pct_revenue_pickup_aeropuerto,

        -- Viajes que TOCAN un aeropuerto (empiezan o terminan en uno)
        ROUND(100 * SUM(CASE WHEN zp.is_airport OR zd.is_airport THEN 1 ELSE 0 END) / COUNT(*), 2)                  AS pct_viajes_tocan_aeropuerto,
        ROUND(100 * SUM(CASE WHEN zp.is_airport OR zd.is_airport THEN f.total_amount END) / SUM(f.total_amount), 2) AS pct_revenue_tocan_aeropuerto
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zp ON f.pickup_zone_key  = zp.zone_key
    JOIN {T_DM_DIM_ZONE} zd ON f.dropoff_zone_key = zd.zone_key
"""))

# Detalle por aeropuerto de origen
display(spark.sql(f"""
    SELECT zp.zone AS aeropuerto_pickup,
           COUNT(*) AS viajes,
           ROUND(SUM(f.total_amount), 2) AS revenue,
           ROUND(SUM(f.total_amount) / COUNT(*), 2) AS ticket_promedio,
           ROUND(100 * SUM(f.total_amount) / (SELECT SUM(total_amount) FROM {T_DM_FACT_TRIPS}), 2) AS pct_revenue_total
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zp ON f.pickup_zone_key = zp.zone_key
    WHERE zp.is_airport
    GROUP BY zp.zone
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Respuesta P1**
# MAGIC
# MAGIC - **Manhattan Yellow Zone concentra el revenue:** 76,5 % del total del período, con un ticket promedio de $23,45. Le siguen los aeropuertos de Queens (JFK y LaGuardia) con entre 13,8 % y 20,6 % según el mes. Ningún otro borough/service_zone supera el 4,1 % en ningún mes.
# MAGIC - **Participación de los aeropuertos** (`dim_zone.is_airport`, que incluye Newark):
# MAGIC
# MAGIC | Criterio | % de viajes | % de revenue |
# MAGIC |---|---|---|
# MAGIC | Viajes que **empiezan** en un aeropuerto | 6,22 % | **16,49 %** |
# MAGIC | Viajes que **empiezan o terminan** en un aeropuerto | 7,82 % | **21,04 %** |
# MAGIC
# MAGIC - **Pocos viajes, mucho revenue:** un viaje que sale de un aeropuerto deja en promedio **$70,93**, frente a **$23,84** del resto (unas 3 veces más). Son viajes largos hacia Manhattan, con peaje, tarifa fija JFK y `airport_fee`. Uno de cada cinco dólares del período viene de un viaje que toca un aeropuerto.
# MAGIC - **Tendencia:** la participación mensual de los aeropuertos de Queens bajó de 20,6 % (agosto 2024) a 13,8 % (febrero 2025), y se quedó cerca del 15 % en 2025. Una parte se explica por la temporada (el verano tiene más viajes de turismo) y otra porque el resto de la ciudad creció. Por ejemplo, los viajes que salen de Brooklyn pasaron de 24 mil en julio de 2024 a 134 mil en junio de 2025.
# MAGIC - **Newark (EWR)** es marginal como origen: 125 viajes en todo el período ($5.041). Los taxis amarillos casi no recogen pasajeros en Newark, porque queda en Nueva Jersey, fuera de su zona de servicio; ahí aparece sobre todo como destino.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## P2. Ticket promedio de los viajes que terminan en un aeropuerto: días festivos frente a días normales
# MAGIC Ticket promedio = `SUM(total_amount) / COUNT(*)`. "Terminan en aeropuerto" = `dim_zone.is_airport` en el rol de **dropoff**. Los festivos salen de `dim_date.is_holiday`.

# COMMAND ----------

# Festivo vs normal (y normal separado en entre semana / fin de semana, para comparar justo:
# casi todos los festivos federales caen entre semana)
display(spark.sql(f"""
    SELECT
        CASE WHEN d.is_holiday THEN 'Festivo'
             WHEN d.is_weekend THEN 'Normal - fin de semana'
             ELSE 'Normal - entre semana' END                    AS tipo_dia,
        COUNT(DISTINCT d.date_key)                               AS dias,
        COUNT(*)                                                 AS viajes,
        ROUND(COUNT(*) / COUNT(DISTINCT d.date_key), 0)          AS viajes_por_dia,
        ROUND(SUM(f.total_amount) / COUNT(*), 2)                 AS ticket_promedio,
        ROUND(SUM(f.trip_distance) / COUNT(*), 2)                AS distancia_promedio_mi,
        ROUND(SUM(f.tolls_amount) / COUNT(*), 2)                 AS peajes_promedio
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zd ON f.dropoff_zone_key = zd.zone_key
    JOIN {T_DM_DIM_DATE} d  ON f.pickup_date_key  = d.date_key
    WHERE zd.is_airport
    GROUP BY 1
    ORDER BY 1
"""))

# Detalle por aeropuerto de destino
display(spark.sql(f"""
    SELECT
        zd.zone                                                   AS aeropuerto_destino,
        ROUND(SUM(CASE WHEN d.is_holiday     THEN f.total_amount END)
            / SUM(CASE WHEN d.is_holiday     THEN 1 END), 2)      AS ticket_festivo,
        ROUND(SUM(CASE WHEN NOT d.is_holiday THEN f.total_amount END)
            / SUM(CASE WHEN NOT d.is_holiday THEN 1 END), 2)      AS ticket_normal,
        SUM(CASE WHEN d.is_holiday THEN 1 ELSE 0 END)             AS viajes_festivo,
        SUM(CASE WHEN NOT d.is_holiday THEN 1 ELSE 0 END)         AS viajes_normal
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zd ON f.dropoff_zone_key = zd.zone_key
    JOIN {T_DM_DIM_DATE} d  ON f.pickup_date_key  = d.date_key
    WHERE zd.is_airport
    GROUP BY zd.zone
    ORDER BY zd.zone
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Respuesta P2**
# MAGIC
# MAGIC | Tipo de día | Días | Viajes/día | Ticket promedio | Distancia prom. |
# MAGIC |---|---|---|---|---|
# MAGIC | Festivo | 11 | 1.839 | **$71,88** | 12,31 mi |
# MAGIC | Normal (todos) | 354 | 1.917 | **$73,61** | 12,14 mi |
# MAGIC | ↳ entre semana | 250 | 1.961 | $73,98 | 11,98 mi |
# MAGIC | ↳ fin de semana | 104 | 1.813 | $72,66 | 12,52 mi |
# MAGIC
# MAGIC - **En festivos el ticket hacia los aeropuertos es $1,73 más bajo (−2,3 %)** que en un día normal, y $2,10 más bajo que en un día normal entre semana. Pasa en los dos aeropuertos grandes: JFK $80,33 vs $82,11 y LaGuardia $66,17 vs $68,16. Newark sube un poco ($89,49 vs $88,78), pero con solo 196 viajes en festivos no es concluyente.
# MAGIC - **No es porque los viajes sean más cortos:** en festivos la distancia promedio es mayor (12,31 vs 11,98 mi en días entre semana). La diferencia sale de **cómo se cobra**: el taxímetro cobra también por tiempo detenido en tráfico, y los días hábiles tienen el recargo de hora pico (`extra`). En un festivo hay menos congestión y no hay hora pico laboral, así que el mismo trayecto cuesta menos.
# MAGIC - **Los festivos se comportan como un fin de semana**, tanto en ticket ($71,88 vs $72,66) como en volumen: 1.839 viajes por día, contra 1.813 un fin de semana y 1.961 un día entre semana.
# MAGIC - **Para Finanzas:** un festivo baja el volumen hacia los aeropuertos un 6 % y el ticket un 3 % frente a un día hábil, así que la proyección de ingresos de un festivo debería hacerse con el perfil de un fin de semana y no con el de un día hábil.
# MAGIC - **Limitación:** solo hay 11 festivos en el período, así que la comparación por festivo individual (por ejemplo, Thanksgiving frente a Navidad) tendría muestras pequeñas.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## P3. De los viajes con tarifa JFK (`RatecodeID = 2`), ¿qué porcentaje empieza o termina en JFK Airport? ¿Qué explica el resto?

# COMMAND ----------

# Clasificación de los viajes con tarifa JFK según su origen/destino
display(spark.sql(f"""
    WITH jfk_rate AS (
        SELECT f.*, zp.zone AS zona_pickup, zp.borough AS borough_pickup,
                    zd.zone AS zona_dropoff, zd.borough AS borough_dropoff
        FROM {T_DM_FACT_TRIPS} f
        JOIN {T_DM_DIM_RATE_CODE} r ON f.rate_code_key    = r.rate_code_key
        JOIN {T_DM_DIM_ZONE} zp     ON f.pickup_zone_key  = zp.zone_key
        JOIN {T_DM_DIM_ZONE} zd     ON f.dropoff_zone_key = zd.zone_key
        WHERE r.rate_code_key = 2
    )
    SELECT
        CASE WHEN zona_pickup = 'JFK Airport' AND zona_dropoff = 'JFK Airport' THEN '1. Empieza y termina en JFK'
             WHEN zona_pickup = 'JFK Airport'  THEN '2. Empieza en JFK'
             WHEN zona_dropoff = 'JFK Airport' THEN '3. Termina en JFK'
             ELSE '4. Ni empieza ni termina en JFK' END            AS categoria,
        COUNT(*)                                                  AS viajes,
        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)          AS pct_viajes,
        ROUND(SUM(fare_amount) / COUNT(*), 2)                     AS tarifa_base_promedio,
        ROUND(SUM(trip_distance) / COUNT(*), 2)                   AS distancia_promedio_mi
    FROM jfk_rate
    GROUP BY 1
    ORDER BY 1
"""))

# Para el "resto": ¿de dónde salen y a dónde van?
display(spark.sql(f"""
    SELECT zp.borough AS borough_pickup, zp.zone AS zona_pickup,
           zd.borough AS borough_dropoff, zd.zone AS zona_dropoff,
           COUNT(*) AS viajes,
           ROUND(SUM(f.fare_amount) / COUNT(*), 2)   AS tarifa_base_promedio,
           ROUND(SUM(f.trip_distance) / COUNT(*), 2) AS distancia_promedio_mi
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zp ON f.pickup_zone_key  = zp.zone_key
    JOIN {T_DM_DIM_ZONE} zd ON f.dropoff_zone_key = zd.zone_key
    WHERE f.rate_code_key = 2
      AND zp.zone <> 'JFK Airport' AND zd.zone <> 'JFK Airport'
    GROUP BY 1, 2, 3, 4
    ORDER BY viajes DESC
    LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta P3**
# MAGIC
# MAGIC De los **907.661** viajes con `RatecodeID = 2` (tarifa fija JFK), el **96,6 %** empieza o termina en JFK Airport:
# MAGIC
# MAGIC | Categoría | Viajes | % |
# MAGIC |---|---|---|
# MAGIC | Empieza en JFK | 675.895 | 74,47 % |
# MAGIC | Termina en JFK | 199.046 | 21,93 % |
# MAGIC | Empieza y termina en JFK | 1.833 | 0,20 % |
# MAGIC | **Ni empieza ni termina en JFK** | **30.887** | **3,40 %** |
# MAGIC
# MAGIC La tarifa JFK es un monto fijo de **$70** entre JFK y Manhattan, y todas las categorías lo cobran (`fare_amount` promedio = $70,00). La asimetría 74 % / 22 % muestra que la mayoría de pasajeros toma el taxi amarillo **desde** el aeropuerto (fila de taxis en la terminal); hacia JFK compiten más las apps y el transporte público.
# MAGIC
# MAGIC **¿Qué explica el 3,4 % restante?** Tres causas, que se ven en los pares origen–destino más frecuentes:
# MAGIC
# MAGIC 1. **Imprecisión al asignar la zona (la mayor parte):** la ubicación del viaje sí es JFK, pero quedó en otra zona.
# MAGIC    - **Zona 264 "Unknown"**: el par más frecuente (833 viajes) es Unknown → Unknown con **17,3 millas**, la distancia típica JFK–Manhattan. Son viajes de JFK cuya ubicación no se pudo asignar.
# MAGIC    - **Baisley Park** (Queens), zona vecina a JFK: aparece una y otra vez como origen hacia Times Sq, Midtown y Murray Hill, con 14–15 millas. Lo más probable es que sean recogidas en el borde del aeropuerto (zonas de espera, hoteles, área de carga) que el sistema asigna a la zona contigua.
# MAGIC 2. **Error al marcar la tarifa en el taxímetro:** viajes con $70 de tarifa base que **no pueden ser tarifa JFK**: Times Sq → Times Sq (2,79 mi), Long Island City → Times Sq (2,89 mi), Midtown North → Midtown North, o LaGuardia → Manhattan (LaGuardia no tiene tarifa fija). Si el pasajero de verdad pagó $70 por un trayecto de 3 millas, es un **sobrecobro**.
# MAGIC 3. **Viajes internos del aeropuerto** (1.833, 3,84 mi): entre terminales o a hoteles del aeropuerto, cobrados con tarifa fija.
# MAGIC
# MAGIC **Para Finanzas y calidad de datos:** los casos del punto 2 son candidatos a auditoría. Una regla en silver podría marcar `RatecodeID = 2` cuando ningún extremo está en JFK ni en sus zonas vecinas, y la distancia es menor a unas 8 millas. El mart permite encontrarlos con una consulta simple sobre `dim_rate_code` y los dos roles de `dim_zone`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4. Propina promedio con tarjeta por `day_part` y fin de semana. ¿Cuándo se deja más propina?
# MAGIC Solo viajes donde `dim_payment_type.tip_is_recorded` (tarjeta), la regla que vive en la dimensión. Se muestran dos métricas:
# MAGIC - **Propina promedio por viaje** = `SUM(tip_amount) / COUNT(*)` (en dólares)
# MAGIC - **Tasa de propina** = `SUM(tip_amount) / SUM(fare_amount)` (% sobre la tarifa), calculada desde sus componentes y no promediando `tip_rate`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        t.day_part                                                    AS franja_horaria,
        CASE WHEN d.is_weekend THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
        COUNT(*)                                                      AS viajes_tarjeta,
        ROUND(SUM(f.tip_amount) / COUNT(*), 2)                        AS propina_promedio,
        ROUND(100 * SUM(f.tip_amount) / SUM(f.fare_amount), 2)        AS tasa_propina_pct,
        ROUND(SUM(f.fare_amount) / COUNT(*), 2)                       AS tarifa_promedio
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
    JOIN {T_DM_DIM_TIME} t         ON f.pickup_hour_key  = t.hour_key
    JOIN {T_DM_DIM_DATE} d         ON f.pickup_date_key  = d.date_key
    WHERE p.tip_is_recorded
    GROUP BY t.day_part, d.is_weekend
    ORDER BY CASE t.day_part WHEN 'madrugada' THEN 1 WHEN 'mañana' THEN 2
                             WHEN 'tarde' THEN 3 ELSE 4 END, d.is_weekend
"""))

# Comparación: lo que daría el promedio "ingenuo" incluyendo efectivo y Flex Fare (propina = 0 no registrada)
display(spark.sql(f"""
    SELECT p.payment_group,
           COUNT(*) AS viajes,
           ROUND(SUM(f.tip_amount) / COUNT(*), 2) AS propina_promedio
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
    GROUP BY p.payment_group
    UNION ALL
    SELECT 'TODOS (sin filtrar)', COUNT(*), ROUND(SUM(tip_amount) / COUNT(*), 2)
    FROM {T_DM_FACT_TRIPS}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Respuesta P4** (29.008.626 viajes con tarjeta)
# MAGIC
# MAGIC | Franja | Tipo de día | Propina promedio | Tasa de propina | Tarifa promedio |
# MAGIC |---|---|---|---|---|
# MAGIC | madrugada | entre semana | **$4,43** | 21,62 % | $20,48 |
# MAGIC | madrugada | fin de semana | $3,42 | 21,65 % | $15,78 |
# MAGIC | mañana | entre semana | $3,92 | 22,69 % | $17,26 |
# MAGIC | mañana | fin de semana | $3,84 | 22,79 % | $16,83 |
# MAGIC | tarde | entre semana | $4,29 | 23,23 % | $18,45 |
# MAGIC | tarde | fin de semana | $4,07 | 22,12 % | $18,38 |
# MAGIC | noche | entre semana | $4,09 | **23,79 %** | $17,19 |
# MAGIC | noche | fin de semana | $4,09 | 22,19 % | $18,42 |
# MAGIC
# MAGIC **¿Cuándo se deja más propina?** Depende de si se mide en dólares o en porcentaje, y la diferencia importa:
# MAGIC - **En dólares, en la madrugada entre semana ($4,43).** Pero se debe a que esos viajes son más caros (tarifa promedio $20,48, la más alta: viajes largos y hacia aeropuertos), no a que los pasajeros sean más generosos. Su tasa es la más baja (21,6 %).
# MAGIC - **En porcentaje, en la noche entre semana (23,79 %) y la tarde entre semana (23,23 %).** Son las franjas de regreso del trabajo y de salidas de negocios, con más pagos con tarjeta corporativa.
# MAGIC - **Entre semana se deja proporcionalmente más que el fin de semana** en la tarde (+1,1 puntos) y la noche (+1,6 puntos). En la mañana y la madrugada la tasa es prácticamente igual.
# MAGIC - **La tasa es muy estable (21,6 %–23,8 %)** porque la pantalla de pago del taxi sugiere porcentajes fijos (20/25/30 %). Por eso la propina en dólares sigue sobre todo a la tarifa.
# MAGIC
# MAGIC **Por qué importa la regla `tip_is_recorded`:** sin filtrar por forma de pago, la propina promedio da **$3,06** en vez de **$4,08**, un 25 % menos, porque se mezclan 4,5 millones de viajes en efectivo con propina 0 registrada (el pasajero puede haberla dado en mano). En el mart la regla vive en `dim_payment_type`, y la vista `gold_taxi_dm_v_propinas_tarjeta` ya la aplica.
# MAGIC
# MAGIC **Hallazgo:** los viajes **Flex Fare** registran una propina promedio de **$0,49**, no 0, así que en una parte de esos viajes la propina sí queda registrada (probablemente las que se pagan dentro de la app). Se mantiene `tip_is_recorded = false` para Flex Fare porque el registro es parcial y mezclarlo sesgaría el promedio hacia abajo. Si Finanzas necesita analizar propinas de Flex Fare, se puede agregar un valor intermedio (por ejemplo, `tip_recording = 'parcial'`) en la dimensión, sin tocar la tabla de hechos.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## P5. Mezcla de formas de pago por mes: ¿qué participación tienen tarjeta, efectivo y Flex Fare?
# MAGIC Se usa `dim_payment_type.payment_group` (Tarjeta, Efectivo, Flex Fare, Otros). La participación se calcula en **viajes** y en **revenue**.

# COMMAND ----------

display(spark.sql(f"""
    WITH base AS (
        SELECT d.year AS anio, d.month AS mes, p.payment_group,
               COUNT(*) AS viajes, SUM(f.total_amount) AS revenue
        FROM {T_DM_FACT_TRIPS} f
        JOIN {T_DM_DIM_DATE} d         ON f.pickup_date_key  = d.date_key
        JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
        GROUP BY d.year, d.month, p.payment_group
    ),
    pct AS (
        SELECT anio, mes, payment_group,
               100 * viajes  / SUM(viajes)  OVER (PARTITION BY anio, mes) AS pct_viajes,
               100 * revenue / SUM(revenue) OVER (PARTITION BY anio, mes) AS pct_revenue
        FROM base
    )
    SELECT anio, mes,
        ROUND(MAX(CASE WHEN payment_group = 'Tarjeta'   THEN pct_viajes END), 1) AS tarjeta_pct_viajes,
        ROUND(MAX(CASE WHEN payment_group = 'Efectivo'  THEN pct_viajes END), 1) AS efectivo_pct_viajes,
        ROUND(MAX(CASE WHEN payment_group = 'Flex Fare' THEN pct_viajes END), 1) AS flexfare_pct_viajes,
        ROUND(MAX(CASE WHEN payment_group = 'Otros'     THEN pct_viajes END), 1) AS otros_pct_viajes,
        ROUND(MAX(CASE WHEN payment_group = 'Tarjeta'   THEN pct_revenue END), 1) AS tarjeta_pct_revenue,
        ROUND(MAX(CASE WHEN payment_group = 'Efectivo'  THEN pct_revenue END), 1) AS efectivo_pct_revenue,
        ROUND(MAX(CASE WHEN payment_group = 'Flex Fare' THEN pct_revenue END), 1) AS flexfare_pct_revenue
    FROM pct
    GROUP BY anio, mes
    ORDER BY anio, mes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Respuesta P5**
# MAGIC
# MAGIC | | Jul 2024 | Dic 2024 | Jun 2025 | Cambio |
# MAGIC |---|---|---|---|---|
# MAGIC | **Tarjeta** (% viajes) | 75,6 % | 77,8 % | 65,2 % | −10,4 pts |
# MAGIC | **Efectivo** (% viajes) | 14,6 % | 13,0 % | 8,9 % | −5,7 pts |
# MAGIC | **Flex Fare** (% viajes) | 9,4 % | 8,6 % | **25,4 %** | **+16,0 pts** |
# MAGIC | Otros (% viajes) | 0,5 % | 0,5 % | 0,4 % | — |
# MAGIC
# MAGIC - **Flex Fare es el gran cambio del período:** estable entre 9 % y 13 % durante 2024, salta a ~20 % en febrero de 2025 y llega a **25,4 % en junio de 2025**. Uno de cada cuatro viajes ya es Flex Fare (viajes pedidos por app con precio acordado por adelantado). Crece a costa de los dos medios tradicionales: la tarjeta cae 10 puntos y el efectivo casi 6.
# MAGIC - **El efectivo sigue bajando:** de 14,6 % a 8,9 % de los viajes en un año.
# MAGIC - **Viajes vs revenue:** la tarjeta pesa más en revenue que en viajes (78,4 % vs 75,6 % en julio de 2024) porque se usa en viajes más caros (aeropuertos, distancias largas). El efectivo pesa menos en revenue (12,8 % vs 14,6 %): viajes cortos y baratos. Flex Fare pesa un poco menos en revenue que en viajes, aunque en junio de 2025 ya está casi igual (25,2 % vs 25,4 %).
# MAGIC
# MAGIC **Implicaciones para Finanzas y para el mart:**
# MAGIC - **Cada vez hay menos información por viaje.** Flex Fare llega sin `RatecodeID`, `passenger_count`, `congestion_surcharge` ni `airport_fee`. Si la tendencia sigue, en 2025 una cuarta parte de los viajes no tiene esos datos. Las métricas de recargos (P6) y de pasajeros cubren cada vez una porción menor del total, y eso hay que reportarlo siempre junto a la cifra.
# MAGIC - **Propinas:** el análisis de propinas solo cubre tarjeta, que bajó del 78 % al 65 % de los viajes. Las conclusiones de P4 representan cada vez a menos pasajeros.
# MAGIC - Por eso las decisiones de diseño (NULL y no 0, miembro `-1` para la tarifa) pesan más con el tiempo: con 0 los promedios estarían cada vez más sesgados.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## P6. Peso de los recargos (`congestion_surcharge + airport_fee + cbd_congestion_fee`) sobre `total_amount` por borough de pickup
# MAGIC Peso = `SUM(recargos) / SUM(total_amount)`. Se suman las columnas por separado con `SUM`, que ignora NULL, para no anular una fila completa por un NULL. Se separa por período, porque `cbd_congestion_fee` solo existe desde enero de 2025, y se muestra también el peso **excluyendo Flex Fare**, porque esos viajes no traen `congestion_surcharge` ni `airport_fee`.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        zp.borough                                                       AS borough_pickup,
        CASE WHEN d.year = 2024 THEN '2024-S2 (sin CBD)' ELSE '2025-S1 (con CBD)' END AS periodo,
        COUNT(*)                                                         AS viajes,
        ROUND(SUM(f.total_amount), 0)                                    AS revenue,
        ROUND(100 * SUM(f.congestion_surcharge) / SUM(f.total_amount), 2) AS pct_congestion,
        ROUND(100 * SUM(f.airport_fee)          / SUM(f.total_amount), 2) AS pct_airport_fee,
        ROUND(100 * COALESCE(SUM(f.cbd_congestion_fee), 0) / SUM(f.total_amount), 2) AS pct_cbd,
        ROUND(100 * (COALESCE(SUM(f.congestion_surcharge), 0)
                   + COALESCE(SUM(f.airport_fee), 0)
                   + COALESCE(SUM(f.cbd_congestion_fee), 0)) / SUM(f.total_amount), 2) AS pct_recargos_total,
        -- Mismo cálculo excluyendo Flex Fare (que no reporta congestion ni airport_fee)
        ROUND(100 * (COALESCE(SUM(CASE WHEN p.payment_group <> 'Flex Fare' THEN f.congestion_surcharge END), 0)
                   + COALESCE(SUM(CASE WHEN p.payment_group <> 'Flex Fare' THEN f.airport_fee END), 0)
                   + COALESCE(SUM(CASE WHEN p.payment_group <> 'Flex Fare' THEN f.cbd_congestion_fee END), 0))
              / SUM(CASE WHEN p.payment_group <> 'Flex Fare' THEN f.total_amount END), 2) AS pct_recargos_sin_flex
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_ZONE} zp        ON f.pickup_zone_key  = zp.zone_key
    JOIN {T_DM_DIM_DATE} d         ON f.pickup_date_key  = d.date_key
    JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
    GROUP BY zp.borough, periodo
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Respuesta P6**
# MAGIC
# MAGIC | Borough pickup | Recargos / total 2024-S2 | Recargos / total 2025-S1 | Sin Flex Fare 2024-S2 → 2025-S1 |
# MAGIC |---|---|---|---|
# MAGIC | **Manhattan** | 9,48 % | **11,23 %** | 10,50 % → **13,14 %** |
# MAGIC | Queens | 4,28 % | 4,51 % | 4,39 % → 4,94 % |
# MAGIC | Brooklyn | 0,97 % | 0,97 % | 3,44 % → 4,17 % |
# MAGIC | Bronx | 0,31 % | 0,36 % | 1,80 % → 1,99 % |
# MAGIC | Staten Island | 0,16 % | 0,29 % | 0,51 % → 0,69 % |
# MAGIC
# MAGIC - **Manhattan es donde más pesan los recargos:** casi 1 de cada 10 dólares en 2024 y más de 1 de cada 9 en 2025. Casi todo es `congestion_surcharge` (8,7 %–9,5 %), que se cobra en los viajes que empiezan, terminan o pasan por Manhattan al sur de la calle 96.
# MAGIC - **El cargo CBD (desde el 5 de enero de 2025) sumó 2,53 puntos en Manhattan**, que pasó de 9,48 % a 11,23 % del total. En los demás boroughs su efecto es menor (0,2 a 0,6 puntos), porque solo se cobra cuando el viaje entra a la zona de congestión de Manhattan.
# MAGIC - **Queens tiene un perfil distinto:** el `airport_fee` (2,1 %–2,3 %) pesa más que el de congestión, porque se cobra en los viajes que salen de JFK y LaGuardia.
# MAGIC - **Zona Unknown (264)** tiene un peso de recargos similar al de Manhattan (7 %–9 %), lo que sugiere que buena parte de esos viajes son trayectos dentro de Manhattan cuya zona no se pudo asignar.
# MAGIC
# MAGIC **Cuidado con Flex Fare:** esos viajes no reportan `congestion_surcharge` ni `airport_fee` (NULL), pero sí cuentan en `total_amount`. Eso **diluye** el peso aparente de los recargos, sobre todo en Brooklyn: con todos los viajes da 0,97 %, y excluyendo Flex Fare da 3,4 %–4,2 %. Brooklyn pasó de 210 mil a 547 mil viajes entre los dos semestres, y buena parte del crecimiento fue Flex Fare. Por eso, para medir el peso real de los recargos, la cifra confiable es la de la columna **sin Flex Fare**. Esto confirma la decisión de dejar NULL en vez de 0: con 0 no habría forma de distinguir "no pagó recargo" de "no se reportó".

# COMMAND ----------

# MAGIC %md
# MAGIC - 