# ETL with Python

## Description

ETL pipeline that processes e-commerce data to generate sales metrics. Built in Python using pandas, with a focus on data cleaning, normalization, and proper data typing.

The pipeline is designed to run in isolated environments (Docker or virtual environment).

---

## Tech stack

- Python
- pandas
- numpy
- pyarrow
- Docker

---

## Dataset

The project works with multiple CSV files that make up an e-commerce system (orders, customers, products, etc.). The input files follow a predefined schema (file and field names) expected by the pipeline.

---

## Project structure

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
data/    # inputs (non-versioned)
output/  # results (non-versioned)

dockerfile
.dockerignore
.gitignore
README.md
requirements.txt
```

* `src/`: pipeline code (extract, transform, load, analytics, main, init)
* `data/`: input data (non-versioned)
* `output/`: results (non-versioned)
* `config.py`: path configuration and output formats

---

## Pipeline flow

Extract → Transform → Analytics → Load

---

## How to run (local)

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\Activate
pip install -r requirements.txt
```

Execute pipeline:

```bash
python -m src.main
```

---

## How to run (Docker)

The ETL process expects CSV files in the `data/` directory and will write the results to `output/`.


```bash
docker build -t etl-project.
docker run --rm -v "%cd%\data":/app/data -v "%cd%\output":/app/output etl-project
```

---

## Extract

* Reading CSV files
* Load validation

---

## Transform

* Normalization of nulls
* Cleaning of empty strings
* Data type conversion considering primary and foreign keys to maintain relational consistency
* Date conversion
* Optional conversion of Period → timestamp for compatibility with BI tools and Parquet

---

## Cleaning decisions

### Nulls

* DF_ORDERS: Replacing nulls in `notes` with “No notes,” since it is an optional field.
* DF_ORDERS: Retaining nulls in `promotion_id` because they represent the absence of a relationship (FK).
* DF_CATEGORIES: `parent_category_id` retains nulls since they identify parent categories.

### Duplicates

As a test, duplicates were evaluated excluding PK. It was verified that duplicates in `order_items` correspond to multiple products within the same order.

### Types

Reusable functions were created:

* `cast_columns`
* `cast_to_date`

The conversions take into account data functionality, future loads, and analysis.

---

## Output

Results are saved in CSV and/or Parquet format depending on the configuration in `src/config.py`.

Examples:

* top_clients
* most_sold_product
* monthly_sales

The pipeline enables reproducible export for analytical and BI use.

---

## Pipeline design

* Centralized configuration
* Configurable export
* Supports local execution and Docker
* Inputs and outputs decoupled from the repository for reproducibility

---

## Potential future improvements

* More robust error handling


## Author

Gaston Rodriguez
