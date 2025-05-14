# 📊 Databricks Data Pipeline with Delta Lake (Course Practice)

This project implements an end-to-end **data engineering pipeline** on **Databricks**, based on best practices and a course exercise from Datalytics. It covers ingestion, validation, cleansing, transformation, optimization, and advanced analytics using **Delta Lake**.


## Project Overview

The pipeline is divided into multiple structured stages:

* **Data Lake (Pre-Landing Zone)**:
  External tables created from raw CSV files, defining explicit schema without inferring metadata.

* **Data Sandbox**:
  Data cleansing, referential integrity validation, and creation of internal tables in Avro and Parquet formats.

* **Consume Zone (Data Warehouse)**:
  Partitioned tables, calculation of new fields (e.g., total sales amount), and data curation for analytical purposes.

* **Advanced Analytics**:
  Business queries to calculate KPIs like Top Sellers, Top Branches, and Lowest Performing Branches.

* **Sales Data Optimization**:
  Parameterized queries to filter by region and date ranges, with dynamic optimization.

* **Advanced Operations and Delta Tables**:
  Implementation of a MERGE operation (INSERT, UPDATE, DELETE) based on an `update_mode` parameter.
  Creation of a Delta Table to guarantee ACID transactions and data versioning.


## Repository Structure

```
databricks-data-pipeline/
├── notebooks/
│   ├── 01_Data_Lake.sql
│   ├── 02_Data_Sandbox.sql
│   ├── 03_Consume_Zone.sql
│   ├── 04_Consultas_Analiticas.sql
│   ├── 05_sales_data.sql
│   └── 06_Operaciones_Avanzadas.sql
├── data/
│   ├── empleados.csv
│   ├── fact.csv
│   ├── locales.csv
│   └── producto.csv
├── docs/
│   └── Documentation.md
├── README.md
└── .gitignore
```

* `notebooks/`: SQL scripts organized by processing stage.
* `data/`: Raw CSV files used as the initial source.
* `docs/`: Extended documentation explaining the pipeline methodology.


## Technologies Used

* **Databricks SQL**: Main development environment.
* **Delta Lake**: Data format ensuring ACID properties and high performance.
* **Apache Spark (Databricks Runtime)**: For distributed query execution.
* **Avro & Parquet**: Formats for internal tables.
* **GitHub**: Version control and project hosting.


## Raw Data Sources

| File            | Description           | Main Columns                                    |
| --------------- | --------------------- | ----------------------------------------------- |
| `empleados.csv` | Employees master data | id\_vendedor, sucursal, nombre                  |
| `fact.csv`      | Sales transactions    | timestamp, sku, vendedor, cantidad              |
| `locales.csv`   | Store master data     | id\_sucursal, nombre, tipo                      |
| `producto.csv`  | Products master data  | id\_producto, familia, nombre, precio\_unitario |

> Join conditions:
>
> * SKU = id\_producto
> * id\_vendedor = vendedor
> * id\_sucursal = sucursal


## Key Tasks Implemented

* Create external tables manually defining schema.
* Validate data ingestion and raw access.
* Ensure referential integrity across fact and dimension tables.
* Create curated internal tables:

  * `fact_ventas` (Avro)
  * `dim_vendedor`, `dim_producto` (Parquet)
* Partition `fact_ventas` by month and add total sales calculation.
* Truncate corrupted partitions.
* Analytical queries to:

  * Find top 10 branches by sales.
  * Find top 3 best-selling products by vendor.
  * Identify bottom 3 performing branches.
* Create parameterized queries for dynamic region and date filtering.
* Implement `MERGE INTO` logic based on `update_mode` (INSERT, UPDATE, DELETE).
* Create and manage a Delta Table with versioning and ACID support.
* Externalize a Delta Table using storage directories.


## KPIs Calculated

* Top-10 branches by sales amount.
* Top-3 products sold by each vendor.
* Worst-performing 3 branches.
* Regional sales totals, averages per client, and transaction counts.


## How to Run

1. Clone this repository:

```bash
git clone https://github.com/tu_usuario/databricks-data-pipeline.git
```

2. Open Databricks workspace.
3. Upload `.sql` files into your **Repos** or **Workspace**.
4. Upload `.csv` files into `/FileStore/tables/` or adjust paths in notebooks.
5. Run notebooks sequentially:

   * 01 → 02 → 03 → 04 → 05 → 06
6. Validate the results at each stage.
