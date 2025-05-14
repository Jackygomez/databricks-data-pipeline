-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Proyecto de Almacenamiento - Pre-Landing Zone (Data Lake)
-- MAGIC #### Crear la zona de almacenamiento inicial (Pre-Landing o Data Lake) mediante la creación de tablas externas apuntando a archivos CSV, definiendo explícitamente su metadata, y validando la disponibilidad de los datos para posteriores transformaciones.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Carga de archivos crudos

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /FileStore/tables/

-- COMMAND ----------

-- MAGIC %fs head /FileStore/tables/empleados.csv

-- COMMAND ----------

-- MAGIC %fs head /FileStore/tables/fact.csv

-- COMMAND ----------

-- MAGIC %fs head /FileStore/tables/locales.csv

-- COMMAND ----------

-- MAGIC %fs head /FileStore/tables/producto.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###1.1 Validación técnica de los archivos 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_empleados = spark.read.option("header", "true").option("delimiter", ",").csv("/FileStore/tables/empleados.csv")
-- MAGIC df_empleados.printSchema()
-- MAGIC df_empleados.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fact = spark.read.option("header", "true").option("delimiter", ";").csv("/FileStore/tables/fact.csv")
-- MAGIC df_fact.printSchema()
-- MAGIC df_fact.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_locales = spark.read.option("header", "true").option("delimiter", ";").csv("/FileStore/tables/locales.csv")
-- MAGIC df_locales.printSchema()
-- MAGIC df_locales.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_producto = spark.read.option("header", "true").option("delimiter", ";").csv("/FileStore/tables/producto.csv")
-- MAGIC df_producto.printSchema()
-- MAGIC df_producto.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  2. Crear tablas externas BRONZE con SparkSQL (definiendo metadata) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Tabla externa: bronze_empleados

-- COMMAND ----------

-- DBTITLE 1,Tabla externa: empleados_ext
CREATE TABLE IF NOT EXISTS bronze_empleados
(
    id_vendedor INT,
    sucursal INT,
    nombre STRING
)
USING CSV
OPTIONS (
    header = true,
    delimiter = ',',
    inferSchema = false,
    path = '/FileStore/tables/empleados.csv'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Validar contenido: bronze_empleados

-- COMMAND ----------

SELECT * 
FROM bronze_empleados 
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.3 Tabla externa: bronze_fact 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_fact 
(
    timestamp STRING,
    sku INT,
    vendedor INT,
    cantidad INT
)
USING CSV
OPTIONS (
    header = true,
    delimiter = ';',
    inferSchema = false,
    path = '/FileStore/tables/fact.csv'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.4 Validar contenido: bronze_fact

-- COMMAND ----------

SELECT *
FROM bronze_fact 
LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS bronze_fact;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.5 Tabla externa: bronze_locales 

-- COMMAND ----------

-- DBTITLE 1,Tabla externa: locales_ext
CREATE TABLE IF NOT EXISTS bronze_locales 
(
    id_sucursal INT,
    nombre STRING,
    tipo STRING
)
USING CSV
OPTIONS (
    header = true,
    delimiter = ';',
    inferSchema = false,
    path = '/FileStore/tables/locales.csv'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.6 Validar contenido: bronze_locales 

-- COMMAND ----------

SELECT * 
FROM bronze_locales  
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.7 Tabla externa: bronze_producto

-- COMMAND ----------

-- DBTITLE 1,abla externa: producto_ext
CREATE TABLE IF NOT EXISTS bronze_producto
(
    id_producto INT,
    familia STRING,
    nombre STRING,
    precio_unitario DOUBLE
)
USING CSV
OPTIONS (
    header = true,
    delimiter = ';',
    inferSchema = false,
    path = '/FileStore/tables/producto.csv'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.8 Validar contenido: bronze_producto

-- COMMAND ----------

SELECT * 
FROM bronze_producto 
LIMIT 10;