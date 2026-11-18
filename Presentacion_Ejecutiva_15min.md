# Presentación Ejecutiva (Sustentación de 15 Minutos)
## Consultoría Analítica para los Tomadores de Decisiones de la Ciudad de Nueva York
**Metodología CRISP-DM · Apache Spark MLlib (`spark://10.195.34.34:7077`)**

---

### Distribución Sugerida de Tiempos (15 minutos exactos — 4 integrantes)
* **Bloque 1 (Min 0:00 – 3:30 | Integrante 1):** Contexto de Negocio (CRISP-DM Fases I y II), Arquitectura en Clúster Spark/HDFS y Numeral 1 (Filtros, Transformaciones y Tasas por 100k hab.).
* **Bloque 2 (Min 3:30 – 7:30 | Integrante 2):** Numeral 2 — Respuestas a las Preguntas de Negocio (Seguridad Ciudadana NYPD, Pobreza y Siniestralidad Vial *Vision Zero*).
* **Bloque 3 (Min 7:30 – 11:30 | Integrante 3):** Numerales 3, 4, 5 y 6 — Preparación de Datos para Modelado y Resultados de Regresión Logística (Pregunta 8) y K-Means en Spark MLlib.
* **Bloque 4 (Min 11:30 – 15:00 | Integrante 4):** Bonos de Excelencia (Web Scraping PUMA, API Climática y Red Neuronal Profunda MLP) + Plan de Acción Ejecutivo para la Alcaldía de NY.

---

## Estructura Diapositiva por Diapositiva

### Diapositiva 1: Portada Ejecutiva (0:00 – 0:30)
* **Título:** Diagnóstico Territorial, Criminalidad y Accidentalidad Vial en Nueva York: Soporte a la Decisión Pública con Apache Spark.
* **Subtítulo:** Consultoría Analítica bajo Metodología CRISP-DM.
* **Mensaje clave:** Cómo transformar más de **2.5 millones de registros** de tres agencias municipales y dos fuentes externas en decisiones focalizadas por cuadrícula de $1\text{ km}^2$.

### Diapositiva 2: Problema de Negocio y Arquitectura en el Clúster Spark (0:30 – 1:45)
* **Tres clientes institucionales de NYC:**
  1. **NYPD (*CompStat*):** Optimizar patrullaje según severidad penal (`F`, `M`, `V`) y ciclo semanal.
  2. **NYC DOT (*Vision Zero*):** Reducir muertes viales (especialmente en peatones y ciclistas).
  3. **NYC Opportunity:** Cruzar vulnerabilidad socioeconómica (`PWGTP`, pobreza y desempleo) con riesgo físico e institucional.
* **Infraestructura Big Data (`clusterHPC`):**
  * **Apache Spark v3.5.5** (`spark://10.195.34.34:7077`, monitoreo en `http://clusterHPC:37006`) + **Hadoop HDFS** (`hdfs://10.195.34.34:9000`).
  * Volumetría: **2,305,623** colisiones viales + **141,870** arrestos (2026-I) + **68,273** registros ponderados de pobreza + **55** PUMA de salud + **546** días de clima.

### Diapositiva 3: Numeral 1 — Filtros, Transformaciones y el Cambio de Ranking por 100k hab. (1:45 – 3:30)
* **Visual a incluir:** `imagenes/fig_cell_43_1.png` (Tasas por 100,000 hab.) y `imagenes/fig_cell_46_1.png` (Severidad depurada).
* **4 Filtros clave:** Lectura multilínea y descarte de filas desplazadas/corruptas (eliminando el outlier falso de 3.29M de ciclistas heridos $\rightarrow$ `268` filas descartadas, quedando `2,305,355` colisiones limpias), coherencia aritmética de víctimas, `dropDuplicates` por llave primaria y pesos muestrales `PWGTP > 0`.
* **6 Transformaciones clave:** Conversión de separadores locales, imputación jerárquica de `LAW_CAT_CD` por moda condicionada al código de delito `PD_CD` (`Window.partitionBy`, reduciendo de `1,394` a `0` nulos), codificación de ausencia estructural (`SIN_VEHICULO_3..5` y `-1`), validación espacial `coord_valida` (`2,056,013` colisiones georreferenciadas) y estandarización poblacional con `PWGTP`.
* **Insight ejecutivo:** En volumen bruto Brooklyn lidera en arrestos (`42,877`), pero al dividir por población, **El Bronx pasa al #1 con `2,354.5` arrestos por cada 100,000 hab.** En cambio, en colisiones Brooklyn lidera tanto en volumen (`26,712`) como en tasa (`1,051.4 / 100k`). En el histórico limpio (`779,372` heridos y `3,746` fallecidos), **los peatones ponen el 49.3% de los muertos (`1,847`)**.

### Diapositiva 4: Numeral 2 (Parte A) — Patrones de Seguridad Pública y Pobreza (Preguntas 1, 2 y 6) (3:30 – 5:30)
* **Visuales a incluir:** `imagenes/fig_q1_felonias_dia_boro.png`, `imagenes/fig_cell_51_2.png` y `imagenes/fig_cell_63_1.png`.
* **Pregunta 1 (Ciclo semanal de Felonías `F`, `55,651` casos):** Entre el **52.6% y el 53.6%** de los arrestos por delitos graves se concentran en solo 3 días (**martes a jueves**). El pico rota por condado: **Martes en el Bronx** (`2,440`), **Miércoles en Brooklyn** (`3,101`) y **Queens** (`2,161`), y **Jueves en Manhattan** (`2,213`).
* **Pregunta 2 (Pobreza vs. Delitos Menores `M`):** Al estandarizar por 100,000 hab., la correlación sube de $r = 0.39$ a **$r = 0.60$**; El Bronx lidera en pobreza ponderada (**27.12%**) y en tasa de arrestos menores (**1,388.1 / 100k**).
* **Pregunta 6 (Perfil demográfico por gravedad):** El grupo de 25 a 44 años concentra el 57.5% de las detenciones, con mayor proporción relativa de jóvenes (`<18` y `18-24`) en felonías (`F`) que en delitos menores (`M`).

### Diapositiva 5: Numeral 2 (Parte B) — Vision Zero y las 5 Zonas Calientes Duales (Preguntas 3, 4, 5 y 7) (5:30 – 7:30)
* **Visuales a incluir:** `imagenes/fig_cell_54_1.png`, `imagenes/fig_cell_60_1.png` y `imagenes/fig_cell_66_1.png`.
* **Pregunta 3 (Peatones y ciclistas):** Dos causas humanas dominan: **Distracción del conductor (`1,686`)** y **No ceder el paso (`1,243`)**. Por tipo de vehículo, las **SUV (`1,741`)** superan a los sedanes (`1,554`).
* **Pregunta 4 (Desempleo y educación vs. fatalidad):** Queens (`1.38`), Bronx (`1.37`) y Manhattan (`1.35` fatales / 100k) tienen la misma tasa letal pese a realidades educativas y laborales opuestas $\rightarrow$ exige análisis microterritorial por cuadrícula.
* **Pregunta 5 (Top 5 Hotspots Duales en celdas de $0.01^\circ \times 0.01^\circ$):**
  * **#1 Bronx Central** `(40.845°, -73.905°)`: **1,584 arrestos y 2 choques fatales** (índice `0.9674`).
  * **#2 Brooklyn Este** `(40.675°, -73.905°)`: **1,107 arrestos y 2 choques fatales** (índice `0.9511`).
* **Pregunta 7 (Efecto Manhattan vs. Staten Island):** Staten Island tiene picos pendulares agudos a las **07:00–08:00 h** y **15:00 h** (`7.13%`), mientras Manhattan sostiene una meseta comercial con pico a las **16:00 h** (`507` choques) y alta actividad nocturna (**21:00–01:00 h**).

### Diapositiva 6: Numerales 3 y 4 — Selección de Técnicas MLlib y Preparación de Datos (7:30 – 9:00)
* **Numeral 3:**
  * *Supervisado:* **Regresión Logística** (probabilidad e interpretabilidad de coeficientes para fatalidad vial).
  * *No Supervisado:* **K-Means** (segmentación de cuadrículas urbanas sin etiqueta previa).
* **Numeral 4:**
  * **4.a Correlación de Pearson en MLlib (`Correlation.corr`):** Ninguna variable supera $|r| \ge 0.90$ (máximo $|r| = 0.35$ entre `lat` y `lon` en supervisado; $|r| = 0.78$ entre `log_colisiones` y `log_arrestos` en K-Means).
  * **4.b Normalización:** `StandardScaler(withMean=True, withStd=True)` ajustado solo sobre entrenamiento (2024) sin *data leakage*, hora cíclica ($\sin, \cos$) y transformación $\ln(1+x)$.
  * **4.c Criterio de negocio:** 347 features tras `OneHotEncoder`; exclusión estricta de columnas `INJURED/KILLED` de las entradas; partición temporal **2024 (Train: 83,698) $\rightarrow$ 2025 (Val: 82,918) $\rightarrow$ 2026-I (Test: 40,373)** con peso de clase $w_1 = 381.18$.

### Diapositiva 7: Numerales 5 y 6 — Evaluación de Regresión Logística (Pregunta 8) y K-Means (9:00 – 11:30)
* **Visuales a incluir:** `imagenes/fig_cell_71_1.png` (Coeficientes por familia) y `imagenes/fig_cell_74_1.png` (Mapa de clústeres K-Means).
* **Regresión Logística (Pruebas con `regParam` $\in \{0.01, 0.10, 1.00\}$ y 7 umbrales):**
  * Óptimo en validación: `regParam = 1.00` (`AUPRC val = 0.00482`) y umbral $\tau = 0.55$.
  * En prueba (2026-I): `AUPRC = 0.0042` (vs. prevalencia `0.0025`), `AUROC = 0.5397`, detectando **50 de las 102 colisiones fatales (Recall = 49.02%)**.
  * **Respuesta Pregunta 8:** Las familias con mayor magnitud de coeficiente son **Tipo de vehículo 1 (`0.5000`)** y **Factor contribuyente 1 (`0.4098`, p. ej. defectos de iluminación vial)**, seguidas por **Hora nocturna (`0.0752`)**.
* **K-Means (Pruebas con $k \in \{2, 3, 4\}$):**
  * Óptimo en **Silhouette = `0.4734` con $k = 2$** (frente a `0.4597` con $k=3$ y `0.3691` con $k=4$).
  * **Grupo 0 (`351` zonas críticas):** `83.8` colisiones y `366.0` arrestos por celda, y **17.3% de choques con peatones/ciclistas** (el doble del Grupo 1: `8.7%`).

### Diapositiva 8: Bonos de Enriquecimiento y Red Neuronal Profunda (MLP en MLlib) (11:30 – 13:30)
* **Visuales a incluir:** `imagenes/fig_mlp_validacion.png`, `imagenes/fig_mlp_vs_logistica.png` y `imagenes/fig_cell_92_1.png`.
* **Bono Enriquecimiento (Scraping + API Clima en Spark):**
  * Extracción de los 55 PUMA del *NYS Cancer Registry* (Staten Island lidera con `514.7` casos / 100k hab.) y 546 días de clima: la temperatura media diaria correlaciona positivamente con los heridos viales (**$r = +0.469$**) y la lluvia reduce los arrestos diarios (**$r = -0.306$**).
* **Bono Aprendizaje Profundo (`MultilayerPerceptronClassifier` en Spark MLlib):**
  * Se evaluaron **6 configuraciones** (arquitecturas `32-16`, `64-32` y `64-32-16` con `50` y `150` iteraciones L-BFGS).
  * La arquitectura profunda de **3 capas ocultas (`64-32-16`, `maxIter = 50`)** alcanzó **`AUROC = 0.7052` en validación y `0.6020` en prueba** (superando el `0.5397` de la Regresión Logística), elevó el `F1` a `0.0076` y **redujo en un 63.8% los falsos positivos** (`5,934` vs. `16,382`).

### Diapositiva 9: Recomendaciones Ejecutivas para los Tomadores de Decisiones de NY (13:30 – 15:00)
1. **Inversión Conjunta Alcaldía (NYPD + DOT) en Hotspots Duales y Grupo 0:** Intervenir de inmediato las cuadrículas `(40.845°, -73.905°)` en el Bronx y `(40.675°, -73.905°)` en Brooklyn con modernización de alumbrado público (atacando el predictor `+0.4098` de defectos de iluminación), cámaras y rediseño peatonal.
2. **Despliegue Policial Escalonado (*NYPD CompStat*):** Concentrar escuadrones de felonías entre martes y jueves (`53.1%` de la carga): martes en el Bronx, miércoles en Brooklyn/Queens y jueves en Manhattan.
3. **Ingeniería Vial Asimétrica (*NYC DOT Vision Zero*):** Priorizar intervalos peatonales adelantados (LPI) en las `351` celdas del Grupo 0 para reducir atropellamientos por SUV (`1,741`) y giros sin ceder el paso (`1,243`), y adaptar semáforos al régimen bimodal de Staten Island (07–08 h y 15 h) frente al régimen continuo/nocturno de Manhattan.
4. **Política Social y Mejora de Datos (*NYC Opportunity*):** Focalizar programas de empleo juvenil (18–44 años) en el Bronx para reducir delitos menores por vulnerabilidad económica, y agregar velocidad límite e iluminación vial al formulario MV-104AN para potenciar el despliegue operativo de la red neuronal en Spark MLlib.
