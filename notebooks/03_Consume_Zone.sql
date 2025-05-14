-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consume Zone
-- MAGIC #### En esta notebook creamos el Data Warehouse inicial para análisis, tomando los datos curados del Data Sandbox y llevándolos a tablas Parquet optimizadas, aplicando limpieza, enriquecimiento de columnas y particionamiento estratégico para consultas eficientes.

-- COMMAND ----------

DROP TABLE IF EXISTS fact_ventas_final;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Crear tabla `fact_ventas_final` en formato Parquet
-- MAGIC Creamos una tabla optimizada:
-- MAGIC - Particionada por `mes`
-- MAGIC - Incluye un nuevo campo `monto_total = cantidad * precio_unitario`
-- MAGIC - Excluye registros de diciembre (mes = 12) debido a datos corruptos
-- MAGIC

-- COMMAND ----------

-- Crear tabla interna fact_ventas_final en Parquet, particionada por mes
CREATE TABLE IF NOT EXISTS fact_ventas_final
USING PARQUET
PARTITIONED BY (mes)
AS
SELECT
  f.fecha,
  f.dia,
  f.mes,
  f.ano,
  f.sku,
  f.vendedor,
  f.cantidad,
  (f.cantidad * p.precio_unitario) AS monto_total
FROM fact_ventas f
INNER JOIN dim_producto p
ON f.sku = p.id_producto
WHERE f.mes != 12;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.2 Verificar la tabla `fact_ventas_final`
-- MAGIC Mostramos un sample para validar las transformaciones.
-- MAGIC

-- COMMAND ----------

SELECT * FROM fact_ventas_final LIMIT 10;
