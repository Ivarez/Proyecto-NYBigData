<div align="center">
  <img src="https://thumb.wikimedia.org/wikipedia/commons/thumb/6/6c/Javeriana.svg/3840px-Javeriana.svg.png" alt="Pontificia Universidad Javeriana" width="220"/>

  # Consultoría Analítica para el Estado de Nueva York
  ### Diagnóstico Territorial, Criminalidad y Accidentalidad Vial (Metodología CRISP-DM)
  **Procesamiento de Datos a Gran Escala — Pontificia Universidad Javeriana**
</div>

---

## Equipo Consultor
* **Andrés Felipe Díaz Torres**
* **Luis Bueno**
* **Iván Álvarez**
* **Julián Díaz Castro**

**Profesor:** John Jairo Corredor Franco, PhD

---

## 1. Resumen Ejecutivo

Este repositorio contiene el entregable final del proyecto de consultoría en analítica de datos a gran escala para los tomadores de decisiones del **Estado y la Ciudad de Nueva York**, estructurado bajo las seis fases de la metodología **CRISP-DM** (*Cross-Industry Standard Process for Data Mining*).

Sobre el clúster académico de **Apache Spark v3.5.5** (`spark://10.195.34.34:7077`) y el sistema de archivos distribuido **Hadoop HDFS** (`hdfs://10.195.34.34:9000`), se procesaron e integraron tres repositorios oficiales de *NYC Open Data* junto con dos fuentes externas:

| Fuente de Datos | Entidad / Origen | Registros | Atributos | Rol Analítico |
| :--- | :--- | ---: | :---: | :--- |
| **NYPD Arrest Data (YTD 2026)** | NYPD (`uip8-fykc`) | 141,870 | 19 | Patrones espaciotemporales de detenciones, severidad penal (`F`, `M`, `V`) y demografía. |
| **Motor Vehicle Collisions: Crashes** | NYPD / NYC DOT (`h9gi-nx95`) | 2,305,623 | 29 | Siniestralidad vial, víctimas fatales y heridas por actor vial, vehículos y factores causales. |
| **NYCgov Poverty Measure Data** | NYC Opportunity (`cts7-vksw`) | 68,273 | 61 | Pobreza municipal, desempleo (`ESR`), escolaridad (`SCHL`) y expansión poblacional (`PWGTP`). |
| **NYS Cancer Registry (Rate by PUMA)** | NYS Dept. of Health (*Web Scraping*) | 55 PUMA | 6 | Tasas de incidencia de cáncer por cada 100,000 hab. a nivel de vecindario (Tableau Public). |
| **Clima Diario NYC (2025–2026)** | OpenWeather / Open-Meteo (*API REST*) | 546 días | 6 | Temperatura mínima/máxima, precipitación y viento máximo diario en NYC. |

---

## 2. Estructura del Repositorio

```text
├── EDA_NY_Project_F_con_bono_DL.ipynb          # Cuaderno principal completo (Fases II a V CRISP-DM + Bonos)
├── Presentacion_Ejecutiva_15min.md             # Estructura y guion de la sustentación ejecutiva (15 min)
├── README.md                                   # Documentación general del repositorio
├── .gitignore                                  # Reglas de exclusión de archivos pesados (>100 MB)
└── Proyecto/
    ├── EDA_Numeral4_NY_Project_F.ipynb         # Copia sincronizada del cuaderno en la carpeta de proyecto
    ├── DatosNY.zip                             # Paquete LaTeX listo para importar directamente en Overleaf
    └── DatosNY/                                # Código fuente modular del informe técnico en LaTeX
        ├── main.tex                            # Archivo maestro LaTeX (compilación modular)
        ├── main_completo_overleaf.tex          # Versión unificada en un solo archivo .tex para Overleaf
        ├── referencias.bib                     # Bibliografía en formato BibTeX (IEEEtran)
        ├── logoPUJ.png                         # Escudo institucional PUJ
        ├── imagenes/                           # Figuras generadas por el pipeline en Spark
        └── secciones/
            ├── 01_entendimiento_negocio.tex        # Fase I CRISP-DM: Objetivos de negocio y minería
            ├── 02_seleccion_datos.tex              # Fase II CRISP-DM: Selección y justificación de fuentes
            ├── 03_coleccion_descripcion.tex        # Arquitectura del clúster Spark/HDFS y diccionarios
            ├── 04_exploracion_datos.tex            # 9 elementos de análisis exploratorio (EDA)
            ├── 05_reporte_calidad.tex              # Diagnóstico de nulos y estrategias de tratamiento
            ├── 06_preguntas_negocio.tex            # Formulación de las 8 preguntas directrices
            ├── 07_limpieza_transformacion.tex      # Fase III — Numeral 1: 4 filtros y 6 transformaciones
            ├── 08_bonos.tex                        # Bonos: Web Scraping (PUMA) y API Climática en Spark
            ├── 09_respuestas_preguntas_negocio.tex # Numeral 2: Respuestas cuantitativas a preguntas 1–7
            ├── 10_modelado_evaluacion_mllib.tex    # Numerales 3, 4 (a,b,c), 5, 6 y Pregunta 8 en MLlib
            ├── 11_bono_aprendizaje_profundo.tex    # Bono Entrega Final: Perceptrón Multicapa (MLP) en MLlib
            └── 12_conclusiones_recomendaciones.tex # Fase VI CRISP-DM: Recomendaciones para Nueva York
```

---

## 3. Arquitectura del Clúster y Ejecución

El cuaderno está configurado para ejecutarse sobre el clúster académico **`clusterHPC`**:
* **Motor distribuido:** Apache Spark `v3.5.5` localizado en `/mnt/sda1/Cluster/Spark` e inicializado mediante `findspark.init("/mnt/sda1/Cluster/Spark")`.
* **Nodo Maestro:** `spark://10.195.34.34:7077` (interfaz web de monitoreo *Spark UI* en `http://clusterHPC:37006`).
* **Sistema de archivos distribuido:** Hadoop HDFS (`/mnt/sda1/Cluster/Hadoop/bin/hadoop fs`) con NameNode en `hdfs://10.195.34.34:9000/ProyectoNY/`.
* **Portabilidad:** Si los archivos CSV se encuentran en el directorio de trabajo local, el cuaderno los detecta automáticamente como respaldo a la ruta HDFS.

---

## 4. Principales Resultados de Modelado en Spark MLlib

1. **Modelo Supervisado Lineal — Regresión Logística Ponderada (`LogisticRegression`):**
   * Entrenado sobre `83,698` colisiones de 2024 (`peso = 381.18` para la clase fatal), validado en 2025 (`82,918` colisiones, óptimo en `regParam = 1.00` y umbral $\tau = 0.55$) y evaluado sobre el primer semestre de 2026 (`40,373` colisiones, prevalencia de `0.25%`).
   * **Métricas en prueba (2026-I):** `AUPRC = 0.0042` (+68% sobre prevalencia), `AUROC = 0.5397`, `Exhaustividad = 49.02%` (`50` de `102` colisiones fatales detectadas).
   * **Importancia por familia (Pregunta 8):** Lideran *Tipo de vehículo 1* (`|β| = 0.5000`) y *Factor contribuyente 1* (`|β| = 0.4098`, destacando defectos de iluminación vial), seguidos por la componente nocturna de la hora (`hora_coseno`, `+0.0752`).
2. **Modelo No Supervisado — Agrupamiento Territorial K-Means (`KMeans`):**
   * Evaluado sobre `699` cuadrículas urbanas de $0.01^\circ \times 0.01^\circ$ ($\ge 10$ colisiones en 2026-I) con $k \in \{2, 3, 4\}$.
   * **Óptimo:** $k = 2$ con **coeficiente de Silhouette = `0.4734`**.
   * **Perfiles:** El **Grupo 0 (`351` zonas de alta prioridad)** promedia `83.8` colisiones y `366.0` arrestos por celda, con el doble de afectación a peatones y ciclistas (`17.3%`) que el **Grupo 1 (`348` zonas suburbanas/periféricas)** (`29.3` colisiones, `35.3` arrestos y `8.7%` vulnerables).
3. **Bono de Aprendizaje Profundo — Perceptrón Multicapa (`MultilayerPerceptronClassifier`):**
   * Exploración de 6 configuraciones (arquitecturas `32-16`, `64-32` y `64-32-16` con `50` y `150` iteraciones L-BFGS).
   * **Mejor configuración:** 3 capas ocultas (`[347, 64, 32, 16, 2]`, `maxIter = 50`, umbral $\tau = 0.85$), que eleva el **AUROC a `0.7052` en validación y `0.6020` en prueba**, mejora el `F1` a `0.0076` y reduce en un **63.8%** los falsos positivos frente a la Regresión Logística.
