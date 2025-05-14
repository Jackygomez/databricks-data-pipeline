-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Warehouse o Consume Zone
-- MAGIC #### Curación inicial y validación de integridad referencial para conformación de Data Warehouse en Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Validación de Integridad Referencial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.1 Validar vendedores inexistentes

-- COMMAND ----------

SELECT DISTINCT f.vendedor
FROM bronze_fact f
LEFT JOIN bronze_empleados e
  ON f.vendedor = e.id_vendedor
WHERE e.id_vendedor IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.2 Validar productos inexistentes

-- COMMAND ----------

SELECT DISTINCT f.sku
FROM bronze_fact f
LEFT JOIN bronze_producto p
  ON f.sku = p.id_producto
WHERE p.id_producto IS NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Limpieza de la tabla de hechos (Curación de bronze_fact)

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/fact_ventas
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Crear tabla fact_ventas en formato AVRO

-- COMMAND ----------

-- Crear tabla limpia fact_ventas en formato AVRO
CREATE TABLE IF NOT EXISTS fact_ventas
USING AVRO
AS
SELECT
  CAST(TO_DATE(timestamp, 'd/M/yyyy') AS DATE) AS fecha,
  DAY(TO_DATE(timestamp, 'd/M/yyyy')) AS dia,
  MONTH(TO_DATE(timestamp, 'd/M/yyyy')) AS mes,
  YEAR(TO_DATE(timestamp, 'd/M/yyyy')) AS ano,
  CAST(sku AS INT) AS sku,
  CAST(vendedor AS INT) AS vendedor,
  CAST(cantidad AS INT) AS cantidad
FROM bronze_fact
WHERE vendedor NOT IN (57, 58)
  AND sku NOT IN (0, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129);



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Validar datos generados

-- COMMAND ----------

SELECT * FROM fact_ventas LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Creación de dimensiones curadas

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/dim_vendedor
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 Dimensión de Vendedores (dim_vendedor)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.1 Crear tabla curada dim_vendedor

-- COMMAND ----------

-- Crear tabla interna dim_vendedor en formato Parquet
CREATE TABLE IF NOT EXISTS dim_vendedor
USING PARQUET
AS
SELECT 
    e.id_vendedor,
    e.nombre AS vendedor_nombre,
    l.nombre AS sucursal_nombre,
    l.tipo AS region_nombre
FROM bronze_empleados e
LEFT JOIN bronze_locales l
ON e.sucursal = l.id_sucursal;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.2 Validar datos

-- COMMAND ----------

SELECT * FROM dim_vendedor LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 Dimensión de Productos (dim_producto)

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/dim_producto

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.1 Crear tabla curada dim_producto

-- COMMAND ----------

-- Crear tabla interna dim_producto en formato Parquet
CREATE TABLE IF NOT EXISTS dim_producto
USING PARQUET
AS
SELECT 
    id_producto,
    familia,
    nombre,
    precio_unitario
FROM bronze_producto;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.2 Validar datos

-- COMMAND ----------

SELECT * FROM dim_producto LIMIT 10;
