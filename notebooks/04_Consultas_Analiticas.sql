-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consultas Analíticas
-- MAGIC #### En esta notebook ejecutamos análisis exploratorios de ventas a partir del Data Warehouse creado en Consume Zone, respondiendo a consultas típicas de negocio para toma de decisiones.

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ## 1. Top-10 sucursales según monto vendido
-- MAGIC Obtener las sucursales con mayor monto de ventas.
-- MAGIC

-- COMMAND ----------

SELECT 
    v.sucursal_nombre,
    SUM(f.monto_total) AS total_vendido
FROM fact_ventas_final f
INNER JOIN dim_vendedor v
ON f.vendedor = v.id_vendedor
GROUP BY v.sucursal_nombre
ORDER BY total_vendido DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Top 3 productos más vendidos por vendedor
-- MAGIC Listar los 3 productos más vendidos por cada vendedor.
-- MAGIC

-- COMMAND ----------

WITH ventas_productos AS (
  SELECT
    f.vendedor,
    p.nombre AS producto,
    SUM(f.cantidad) AS total_vendido,
    ROW_NUMBER() OVER (PARTITION BY f.vendedor ORDER BY SUM(f.cantidad) DESC) AS rn
  FROM fact_ventas_final f
  INNER JOIN dim_producto p
    ON f.sku = p.id_producto
  GROUP BY f.vendedor, p.nombre
)
SELECT
  vendedor,
  producto,
  total_vendido
FROM ventas_productos
WHERE rn <= 3
ORDER BY vendedor, total_vendido DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Peores 3 sucursales según monto vendido
-- MAGIC Identificar las sucursales con menores ventas totales.

-- COMMAND ----------

SELECT
  v.sucursal_nombre,
  SUM(f.monto_total) AS total_vendido
FROM fact_ventas_final f
INNER JOIN dim_vendedor v
  ON f.vendedor = v.id_vendedor
GROUP BY v.sucursal_nombre
ORDER BY total_vendido ASC
LIMIT 3;
