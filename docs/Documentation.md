# Project Documentation - Databricks Data Pipeline


## 1. Introduction

This project implements a **complete data processing pipeline** using **Databricks** and **Delta Lake** technologies.
It follows a multi-zone architecture approach, separating raw ingestion, cleansing, curation, and analytical stages.

The practice was designed to simulate real-world tasks of a data engineer, using sales data and dimension tables.


## 2. Dataset Overview

The pipeline processes the following source files:

| File              | Description               | Columns                                         |
| ----------------- | ------------------------- | ----------------------------------------------- |
| **empleados.csv** | Employees master table    | id\_vendedor, sucursal, nombre                  |
| **fact.csv**      | Sales transactions (fact) | timestamp, sku, vendedor, cantidad              |
| **locales.csv**   | Store master table        | id\_sucursal, nombre, tipo                      |
| **producto.csv**  | Products master table     | id\_producto, familia, nombre, precio\_unitario |

> **Important Join Keys:**
>
> * SKU = id\_producto
> * id\_vendedor = vendedor
> * id\_sucursal = sucursal


## 3. Architecture Overview

### Zones:

| Zone                              | Description                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------- |
| **Data Lake (Pre-Landing)**       | External tables created from CSVs, defining explicit schema.                  |
| **Data Sandbox**                  | Internal tables created after cleansing and validating referential integrity. |
| **Consume Zone (Data Warehouse)** | Curated tables optimized for analytical queries and reporting.                |
| **Advanced Processing**           | Optimization of queries, parameterization, Delta Lake operations.             |


## 4. Pipeline Stages Summary

| Notebook                           | Description                                                                                                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **01\_Data\_Lake.sql**             | Create external tables from raw files. Validate accessibility.                                                                                                   |
| **02\_Data\_Sandbox.sql**          | Validate referential integrity, clean fact data, create Avro and Parquet tables.                                                                                 |
| **03\_Consume\_Zone.sql**          | Create curated fact table partitioned by month. Calculate total sales per transaction. Truncate incorrect partitions.                                            |
| **04\_Consultas\_Analiticas.sql**  | Execute business queries: Top branches, top products per seller, worst performing branches.                                                                      |
| **05\_sales\_data.sql**            | Implement parameterized queries for dynamic region/date filtering to optimize aggregation queries.                                                               |
| **06\_Operaciones\_Avanzadas.sql** | Implement advanced operations using Delta Lake MERGE (INSERT, UPDATE, DELETE) based on update\_mode parameter. Create Delta Tables ensuring ACID and versioning. |


## 5. Key Transformations and Models

### 5.1 Fact Table

* **fact\_ventas (Avro Format)**:

  * Only transactions with valid foreign keys.
  * Decomposed timestamp into day, month, and year fields.

### 5.2 Dimension Tables

* **dim\_vendedor (Parquet Format)**:

  * Combined vendedor and sucursal data.
* **dim\_producto (Parquet Format)**:

  * Copy of the product master table with validated structure.

### 5.3 Curated Fact Table

* **fact\_ventas\_final (Parquet Format, Partitioned)**:

  * Includes new calculated field: `monto_total` = cantidad \* precio\_unitario.
  * Partitioned by `mes` (month) for performance.


## 6. Optimization Techniques (Applied and Recommended)

### 6.1 Applied Techniques

* **Explicit schema definition** when creating external tables to avoid reliance on automatic schema inference.
* **Partitioning** the curated fact table (`fact_ventas_final`) by the `mes` (month) field to improve query performance.
* **Use of Window Functions**:

  * Applied in the analytical queries (`04_Consultas_Analiticas.sql`) to rank top products per vendor using `ROW_NUMBER()`.

### 6.2 Recommended Best Practices (Not yet applied)

* **Vacuuming Delta Tables**:

  * **Why?** In Delta Lake, when operations such as `MERGE`, `UPDATE`, or `DELETE` are performed, the old files are not immediately deleted to enable Time Travel and ACID transaction support.

  * **Impact:** Over time, these obsolete files can accumulate and degrade storage efficiency and query performance.

  * **Good Practice:**
    After completing operations that modify large portions of a Delta table, running the following command is recommended to clean up old data files:

    ```sql
    VACUUM table_name RETAIN 0 HOURS;
    ```

  * **Important:**
    It is generally safer to retain at least **7 days** (Databricks default) unless you are certain that no queries will require old versions (e.g., no time travel needed).

    Example safer command:

    ```sql
    VACUUM table_name;
    ```

  * **Where It Could Be Applied:**
    After executing the `MERGE` operations in the `06_Operaciones_Avanzadas.sql` notebook, to optimize the Delta tables created or modified.


# 📌 Quick Summary:

| Optimization     | Status         | Details                                       |
| ---------------- | -------------- | --------------------------------------------- |
| Explicit Schema  | ✅ Applied      | Data Lake ingestion                           |
| Partitioning     | ✅ Applied      | Fact table partitioned by `mes`               |
| Window Functions | ✅ Applied      | Ranking top products per seller               |
| VACUUM           | 🔵 Recommended | Not applied, but ideal after MERGE operations |


## Final Recommendation

> **Although not explicitly executed in this practice, incorporating a `VACUUM` process would enhance long-term storage efficiency and query performance by cleaning up obsolete data files after major Delta Table operations.**

