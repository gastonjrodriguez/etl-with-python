# ETL con Python

## Descripcion

Pipeline ETL que procesa datos de e-commerce para generar métricas de ventas.
Realizado en Python, utilizando pandas, enfocado en limpieza, normalización y tipado correcto de datos.

El pipeline está diseñado para ejecutarse en entornos aislados (Docker o entorno virtual).

---

## Tech stack

- Python
- pandas
- numpy
- pyarrow
- Docker

---

## Dataset

El proyecto trabaja con múltiples archivos CSV que forman un sistema de e-commerce (orders, customers, products, etc.).
Los archivos de entrada siguen un esquema predefinido (nombres de archivos y campos) esperado por el pipeline.

---

## Estructura del proyecto

```
src/
 ├─ extract.py
 ├─ transform.py
 ├─ load.py
 ├─ analytics.py
 ├─ main.py
 ├─ __init__.py
 └─ config.py

.venv/
data/    # inputs (no versionados)
output/  # resultados (no versionados)

dockerfile
.dockerignore
.gitignore
README.md
requirements.txt
```

* `src/`: código del pipeline (extract, transform, load, analytics, main, init)
* `data/`: datos de entrada (no versionados)
* `output/`: resultados generados (no versionados)
* `config.py`: configuración de paths y formatos de salida

---

## Pipeline flow

Extract → Transform → Analytics → Load

---

## Como correr (local)

Crear entorno virtual e instalar dependencias:

```bash
python -m venv .venv
.venv\Scripts\Activate
pip install -r requirements.txt
```

Ejecutar pipeline:

```bash
python -m src.main
```

---

## Como correr (Docker)

El ETL espera los archivos CSV en el directorio `data/` y escribirá los resultados en `output/`.

```bash
docker build -t etl-proyecto .
docker run --rm -v "%cd%\data":/app/data -v "%cd%\output":/app/output etl-proyecto
```

---

## Extract

* Lectura de archivos CSV
* Validación de carga

---

## Transform

* Normalización de nulos
* Limpieza de strings vacíos
* Conversión de tipos de datos considerando claves primarias y foráneas para mantener coherencia relacional
* Conversión de fechas
* Conversión opcional de Period → timestamp para compatibilidad con herramientas BI y Parquet

---

## Decisiones de limpieza

### Nulos

* DF_ORDERS: reemplazo de nulls en `notes` por "Sin notas", ya que es un campo opcional.
* DF_ORDERS: se mantiene `promotion_id` con nulls porque representa ausencia de relación (FK).
* DF_CATEGORIES: `parent_category_id` conserva nulls ya que identifican categorías padre.

### Duplicados

A modo de prueba se evaluaron duplicados excluyendo PK. Se verificó que duplicados en `order_items` responden a múltiples productos dentro de la misma orden.

### Tipos

Se crearon funciones reutilizables:

* `cast_columns`
* `cast_to_date`

Las conversiones consideran funcionalidad del dato, futuras cargas y análisis.

---

## Output

Los resultados se guardan en formato **CSV y/o Parquet** según configuración en `src/config.py`.

Ejemplos:

* ventas_por_cliente
* ventas_por_mes
* datasets del modelo transformado

El pipeline permite exportación reproducible para consumo analítico y BI.

---

## Diseño del pipeline

* Arquitectura modular (extract → transform → load)
* Configuración centralizada
* Exportación configurable
* Compatible con ejecución local y Docker
* Inputs y outputs desacoplados del repositorio para reproducibilidad

---

## Posibles mejoras a futuro

* Manejo de errores mas robusto


## Autor

Gaston Rodriguez
