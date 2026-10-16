# Data mart dimensional NYC Taxi — capa Gold (Finanzas TLC)

Ejercicio del módulo `06-end-to-end-nyc-taxi` del curso de Big Data, Maestría en Ciencia de Datos (UPB).
Autor: **Sebastián Velasco Ardila**.

Modelo estrella en la capa Gold, construido desde Silver, para que el área de Finanzas de la TLC analice ingresos, propinas y recargos en SQL con nombres legibles y sin acceso a Silver.

## Contenido

| Archivo | Partes del enunciado |
|---|---|
| [`00-setup/config.py`](00-setup/config.py) | Constantes `T_DM_*` agregadas, derivadas de `TABLE_PREFIX` |
| [`08-gold-data-mart/01_dm_dimensiones.py`](08-gold-data-mart/01_dm_dimensiones.py) | **Parte 0** (diseño) y **Parte 1** (6 dimensiones) |
| [`08-gold-data-mart/02_dm_fact_trips.py`](08-gold-data-mart/02_dm_fact_trips.py) | **Parte 2** (tabla de hechos, decisión NULL vs 0, conciliación) |
| [`08-gold-data-mart/03_dm_constraints_vistas_permisos.py`](08-gold-data-mart/03_dm_constraints_vistas_permisos.py) | **Parte 3** (PK/FK con `RELY`, vistas de consumo, permisos) |
| [`08-gold-data-mart/04_dm_preguntas_negocio.py`](08-gold-data-mart/04_dm_preguntas_negocio.py) | **Parte 4** (6 preguntas de negocio, solo con el mart) |
| [`08-gold-data-mart/05_dm_rendimiento.py`](08-gold-data-mart/05_dm_rendimiento.py) | **Parte 5** (silver vs estrella, liquid clustering) y **Parte 6** (DAG) |
| [`evidencia/`](evidencia/) | **Parte 3.3**: `SHOW GRANTS` y capturas de la prueba del compañero (lee las vistas; `fact_trips` y silver dan `INSUFFICIENT_PERMISSIONS`) |

Los notebooks están en formato fuente de Databricks (`.py`). Cada sección tiene un encabezado `%md`, la transformación, una celda de verificación con `assert` y las respuestas a las preguntas en celdas `%md`.

## Datos cargados

- **12 meses de Yellow Taxi: julio 2024 – junio 2025.** Cubre los 11 festivos federales y el inicio de `cbd_congestion_fee` (enero 2025).
- Bronze: 44.921.011 viajes → Silver: **39.689.946 viajes válidos** (688.258 duplicados eliminados, 4.542.807 rechazados por reglas de calidad).
- Ajuste en `02-silver/02_silver_trips_enriched`: se descartan las columnas de zona antes del join, porque `01_silver_trips_clean` ya las escribe. Sin eso, Delta falla con `COLUMN_ALREADY_EXISTS`.

## Modelo estrella

**Grano:** cada fila de `fact_trips` es un viaje (las mismas 39.689.946 filas de silver).

```
                  ┌────────────────────────────────────────┐
                  │               fact_trips               │
                  │        grano: 1 fila = 1 viaje         │
                  ├────────────────────────────────────────┤
[ dim_date ] ◄────┤ pickup_date_key      payment_type_key  ├────► [ dim_payment_type ]
[ dim_time ] ◄────┤ pickup_hour_key      rate_code_key     ├────► [ dim_rate_code ]
              ┌───┤ pickup_zone_key      vendor_key        ├────► [ dim_vendor ]
[ dim_zone ] ◄┤   │                                        │
              └───┤ dropoff_zone_key                       │
                  ├────────────────────────────────────────┤
                  │ 13 medidas aditivas: montos ($),       │
                  │ passenger_count, trip_distance,        │
                  │ trip_duration_min                      │
                  └────────────────────────────────────────┘
```

| Dimensión | Filas | Notas |
|---|---|---|
| `dim_date` | 365 | Generada con `sequence` + `explode`; 11 festivos federales (OPM), calculados por regla |
| `dim_time` | 24 | `day_part` e `is_rush_hour`, validado contra silver |
| `dim_zone` | 266 | Role-playing (pickup y dropoff); `is_airport` (Newark, JFK, LaGuardia), `is_special_zone` (264, 265) y miembro `-1` |
| `dim_payment_type` | 8 | `tip_is_recorded` (solo tarjeta), `payment_group`, miembro `-1` |
| `dim_rate_code` | 7 | `is_airport_rate`, miembro `-1` |
| `dim_vendor` | 3 | Miembro `-1` |

## Decisiones de diseño

- **Ratios fuera de la tabla de hechos:** `tip_rate` y `avg_speed_mph` no son aditivos. Se recalculan como `SUM(numerador) / SUM(denominador)`.
- **Llaves sin valor → miembro `-1`** (left join + `coalesce`). Los 6.021.534 viajes Flex Fare quedan con `rate_code_key = -1` y no se pierde ninguno.
- **Medidas sin dato → NULL, no 0.** Con NULL, `AVG(congestion_surcharge)` = $2,37; con 0 bajaría a $2,01 (−15 %), porque contaría los viajes Flex Fare sin dato como si hubieran pagado $0.
- **PK con `RELY`**, respaldada por la conciliación que valida cero duplicados y cero llaves huérfanas.
- **Liquid clustering** `CLUSTER BY (pickup_date_key, pickup_zone_key)`: la fecha aparece en 5 de las 6 preguntas de negocio y la zona de origen en 4.

## Resultados clave

**Conciliación (falla el notebook si no cuadra):**

| Chequeo | Silver | fact_trips |
|---|---|---|
| `COUNT(*)` | 39.689.946 | 39.689.946 ✓ |
| `SUM(total_amount)` | $1.062.587.818,22 | $1.062.587.818,22 ✓ |
| Llaves huérfanas (7 FK) | — | 0 ✓ |
| Llaves duplicadas (6 dimensiones) | — | 0 ✓ |

**Preguntas de negocio (Parte 4):**

| # | Hallazgo |
|---|---|
| P1 | Los aeropuertos son el 6,2 % de los viajes pero el **16,5 % del revenue** (21,0 % si se cuentan los viajes que empiezan o terminan en uno). Ticket de $70,93 vs $23,84 del resto |
| P2 | Hacia los aeropuertos, en festivos el ticket es **$1,73 más bajo** ($71,88 vs $73,61): hay menos tráfico y no hay recargo de hora pico. Los festivos se comportan como un fin de semana |
| P3 | El **96,6 %** de los viajes con tarifa JFK empieza o termina en JFK. El resto se explica por zonas mal asignadas (Unknown, Baisley Park) y por errores al marcar la tarifa (posibles sobrecobros) |
| P4 | La tasa de propina más alta es la de la **noche entre semana (23,8 %)**. En dólares gana la madrugada entre semana ($4,43), porque esos viajes son más caros |
| P5 | **Flex Fare pasó de 9,4 % a 25,4 % de los viajes** en un año, a costa de la tarjeta (−10 pts) y del efectivo (−6 pts) |
| P6 | Los recargos pesan el **11,2 % del total en Manhattan** en 2025 (9,5 % en 2024); el cargo CBD aportó +2,5 pts. Flex Fare diluye el peso aparente en Brooklyn y el Bronx |

**Rendimiento (Parte 5):** el top 20 de pares origen → destino tomó 1,30 s en la estrella y 1,60 s en silver, con resultado idéntico. La estrella lee menos columnas, agrupa por enteros, y sus dos broadcast joins se hacen después de una agregación parcial.

## Orquestación (Parte 6)

```
silver_enriched ──► dm_dimensiones ──► dm_fact_trips ──► dm_constraints_vistas
                                       (conciliación)    (PK/FK, vistas, grants)
```

La rama del mart corre en paralelo con las tareas `gold_*` y la cadena de ML existentes. El detalle, el fragmento YAML y la mejora write–audit–publish están en `05_dm_rendimiento.py`.

## Cómo correrlo

1. En `00-setup/config.py`, ajustar `CATALOG` y `SCHEMA` al catálogo del curso y al schema propio.
2. Tener corridas las capas bronze y silver del módulo (`01-bronze/`, `02-silver/`).
3. Copiar la carpeta `08-gold-data-mart/` dentro de `06-end-to-end-nyc-taxi/`, al mismo nivel que `00-setup/`, para que funcione `%run ../00-setup/config`.
4. Ejecutar en orden: `01` → `02` → `03`. Los notebooks `04` y `05` son de análisis.
5. En `03`, cambiar `COMPANERO` por el correo del compañero de pareja.
