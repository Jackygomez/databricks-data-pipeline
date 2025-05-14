-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Optimización de Consultas 
-- MAGIC #### En esta notebook creamos un dataset de ventas globales consolidado y desarrollamos consultas dinámicas parametrizadas para facilitar el análisis flexible por región y por rangos de fecha. Además, planteamos alternativas de optimización para el manejo de parámetros en Databricks.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Crear dataset consolidado `global_sales`
-- MAGIC Unimos la información de ventas (`fact_ventas_final`) con la dimensión vendedores (`dim_vendedor`) para enriquecer las transacciones con nombre de sucursal y región.
-- MAGIC

-- COMMAND ----------

-- Crear vista temporal global_sales
CREATE OR REPLACE TEMP VIEW global_sales AS
SELECT
    f.fecha,
    f.dia,
    f.mes,
    f.ano,
    f.sku,
    f.vendedor,
    v.sucursal_nombre,
    v.region_nombre,
    f.cantidad,
    f.monto_total
FROM fact_ventas_final f
INNER JOIN dim_vendedor v
ON f.vendedor = v.id_vendedor;


-- COMMAND ----------

SELECT * FROM global_sales LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Consulta dinámica por región
-- MAGIC Creamos una consulta que permita filtrar fácilmente el total de ventas, promedio de ventas y número de transacciones para cualquier región.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Definir parámetros
-- MAGIC region_param = 'Vecino'  # Región que quieres filtrar
-- MAGIC
-- MAGIC # Consulta SQL dinámica
-- MAGIC query = f"""
-- MAGIC SELECT
-- MAGIC   region_nombre,
-- MAGIC   SUM(monto_total) AS total_ventas,
-- MAGIC   AVG(monto_total) AS promedio_ventas_por_cliente,
-- MAGIC   COUNT(*) AS numero_transacciones
-- MAGIC FROM global_sales
-- MAGIC WHERE region_nombre = '{region_param}'
-- MAGIC GROUP BY region_nombre
-- MAGIC """
-- MAGIC
-- MAGIC # Ejecutar consulta
-- MAGIC spark.sql(query).show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Consulta dinámica con rango de fechas opcional
-- MAGIC Extendemos la consulta para permitir análisis en ventanas de tiempo específicas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Parámetros
-- MAGIC region_param = 'Vecino'
-- MAGIC fecha_inicio = '2019-01-01'
-- MAGIC fecha_fin = '2019-12-31'
-- MAGIC
-- MAGIC # Consulta dinámica considerando rango de fechas
-- MAGIC query = f"""
-- MAGIC SELECT
-- MAGIC   region_nombre,
-- MAGIC   SUM(monto_total) AS total_ventas,
-- MAGIC   AVG(monto_total) AS promedio_ventas_por_cliente,
-- MAGIC   COUNT(*) AS numero_transacciones
-- MAGIC FROM global_sales
-- MAGIC WHERE region_nombre = '{region_param}'
-- MAGIC   AND fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
-- MAGIC GROUP BY region_nombre
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(query).show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Alternativas para optimizar parámetros en Databricks
-- MAGIC
-- MAGIC **En producción o en Databricks Enterprise:**
-- MAGIC
-- MAGIC - Usar dbutils.widgets para crear entradas visuales de parámetros.
-- MAGIC
-- MAGIC - Usar notebooks parametrizados en los Jobs de Databricks para que cada corrida reciba valores dinámicos.
-- MAGIC
-- MAGIC - En ambientes avanzados, usar variables de entorno o pipelines automatizados que pasen los parámetros.
-- MAGIC
-- MAGIC **En Databricks Community Edition:**
-- MAGIC
-- MAGIC - Hacerlo como estamos simulando: usando Python + f-strings para construir consultas dinámicas.
-- MAGIC
-- MAGIC - También podrías construir vistas temporales filtradas dinámicamente antes de cada análisis.
-- MAGIC