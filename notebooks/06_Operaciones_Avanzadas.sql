-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Operaciones Avanzadas en Delta Lake
-- MAGIC

-- COMMAND ----------

SHOW TABLES;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Lectura de archivos CSV originales
-- MAGIC Cargamos los datos crudos a Spark DataFrames desde la ruta en DBFS (`/FileStore/tables/`).
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_bronze_empleados = (spark.read
-- MAGIC   .format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("delimiter", ",")
-- MAGIC   .load("/FileStore/tables/empleados.csv") 
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Leer producto.csv
-- MAGIC df_bronze_producto = (spark.read
-- MAGIC   .format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("delimiter", ";")
-- MAGIC   .load("/FileStore/tables/producto.csv")
-- MAGIC )
-- MAGIC
-- MAGIC # Leer locales.csv
-- MAGIC df_bronze_locales = (spark.read
-- MAGIC   .format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("delimiter", ";")
-- MAGIC   .load("/FileStore/tables/locales.csv")
-- MAGIC )
-- MAGIC
-- MAGIC # Leer fact.csv
-- MAGIC df_bronze_fact = (spark.read
-- MAGIC   .format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .option("delimiter", ";")
-- MAGIC   .load("/FileStore/tables/fact.csv")
-- MAGIC )
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Guardar DataFrames en Delta en la Capa Bronze
-- MAGIC Guardamos los datos leídos como tablas Delta, persistiendo en la estructura de zonas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_bronze_empleados.write.format("delta").mode("overwrite").save("/FileStore/bronze/empleados_delta")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Producto Bronze
-- MAGIC df_bronze_producto.write.format("delta").mode("overwrite").save("/FileStore/bronze/producto_delta")
-- MAGIC
-- MAGIC # Locales Bronze
-- MAGIC df_bronze_locales.write.format("delta").mode("overwrite").save("/FileStore/bronze/locales_delta")
-- MAGIC
-- MAGIC # Fact Bronze
-- MAGIC df_bronze_fact.write.format("delta").mode("overwrite").save("/FileStore/bronze/fact_delta")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Creación de tablas Delta en la Capa Bronze
-- MAGIC Registramos los datos en el catálogo de Databricks como tablas para poder consultarlos con SQL.
-- MAGIC

-- COMMAND ----------

-- Registrar tabla bronze_empleados_delta apuntando a la ubicación de almacenamiento
CREATE TABLE IF NOT EXISTS bronze_empleados_delta
USING DELTA
LOCATION '/FileStore/bronze/empleados_delta';


-- COMMAND ----------

-- Crear tabla bronze_producto_delta
CREATE TABLE IF NOT EXISTS bronze_producto_delta
USING DELTA
LOCATION '/FileStore/bronze/producto_delta';

-- Crear tabla bronze_locales_delta
CREATE TABLE IF NOT EXISTS bronze_locales_delta
USING DELTA
LOCATION '/FileStore/bronze/locales_delta';

-- Crear tabla bronze_fact_delta
CREATE TABLE IF NOT EXISTS bronze_fact_delta
USING DELTA
LOCATION '/FileStore/bronze/fact_delta';


-- COMMAND ----------

SELECT * FROM bronze_empleados_delta LIMIT 10;


-- COMMAND ----------

SELECT * FROM bronze_producto_delta LIMIT 10;

-- COMMAND ----------

SELECT * FROM bronze_locales_delta LIMIT 10;

-- COMMAND ----------

SELECT * FROM bronze_fact_delta LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Silver Layer - Transformaciones de Datos
-- MAGIC
-- MAGIC En esta etapa se realizaron transformaciones para preparar los datos para su uso en modelos analíticos o de negocio:
-- MAGIC
-- MAGIC - Se estandarizaron nombres y formatos.
-- MAGIC - Se agregaron columnas de control como fecha de procesamiento.
-- MAGIC - Se aseguraron llaves de relacionamiento correctas entre empleados, locales, productos y ventas.
-- MAGIC - Se almacenaron las tablas en rutas diferenciadas `/FileStore/silver/` respetando la arquitectura de lagos de datos en Databricks.
-- MAGIC

-- COMMAND ----------

-- Crear tabla silver_empleados_delta a partir de bronze_empleados_delta
CREATE OR REPLACE TABLE silver_empleados_delta
USING DELTA
LOCATION '/FileStore/silver/empleados_delta'
AS
SELECT
  id_vendedor,
  sucursal,
  UPPER(TRIM(nombre)) AS nombre_limpio, -- estandarizar nombre
  CURRENT_TIMESTAMP() AS fecha_procesamiento -- marca de procesamiento
FROM bronze_empleados_delta
WHERE id_vendedor IS NOT NULL
  AND sucursal IS NOT NULL
  AND nombre IS NOT NULL;


-- COMMAND ----------

CREATE OR REPLACE TABLE silver_locales_delta
USING DELTA
LOCATION '/FileStore/silver/locales_delta'
AS
SELECT 
  id_sucursal,
  nombre AS sucursal_nombre,
  tipo AS tipo
FROM bronze_locales_delta
WHERE id_sucursal IS NOT NULL
  AND nombre IS NOT NULL
  AND tipo IS NOT NULL;



-- COMMAND ----------

CREATE OR REPLACE TABLE silver_producto_delta
USING DELTA
LOCATION '/FileStore/silver/producto_delta'
AS
SELECT 
  id_producto,
  familia,
  nombre AS producto_nombre,
  precio_unitario
FROM bronze_producto_delta
WHERE id_producto IS NOT NULL
  AND nombre IS NOT NULL
  AND precio_unitario IS NOT NULL;


-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fact_delta
USING DELTA
LOCATION '/FileStore/silver/fact_delta'
AS
SELECT 
  TO_DATE(timestamp, 'd/M/yyyy') AS fecha,
  DAY(TO_DATE(timestamp, 'd/M/yyyy')) AS dia,
  MONTH(TO_DATE(timestamp, 'd/M/yyyy')) AS mes,
  YEAR(TO_DATE(timestamp, 'd/M/yyyy')) AS ano,
  CAST(sku AS INT) AS sku,
  CAST(vendedor AS INT) AS id_vendedor,
  CAST(cantidad AS INT) AS cantidad
FROM bronze_fact_delta
WHERE timestamp IS NOT NULL
  AND sku IS NOT NULL
  AND vendedor IS NOT NULL
  AND cantidad IS NOT NULL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###4.1. Validaciones

-- COMMAND ----------

SELECT * FROM silver_empleados_delta LIMIT 10;


-- COMMAND ----------

SELECT * FROM silver_fact_delta LIMIT 10;

-- COMMAND ----------

SELECT * FROM silver_producto_delta LIMIT 10;

-- COMMAND ----------

SELECT * FROM silver_locales_delta LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.2. Sustento de decisiones tomadas en la Capa Silver:
-- MAGIC
-- MAGIC - Se eliminaron columnas sobrantes para optimizar el almacenamiento y consultas.
-- MAGIC - Se estandarizaron nombres en mayúscula para evitar errores en futuras uniones (joins).
-- MAGIC - Se agregaron columnas de fecha de procesamiento para trazabilidad de los datos.
-- MAGIC - Se dividió la columna de fecha de ventas en componentes día, mes y año para facilitar consultas de agregación.
-- MAGIC - Todas las tablas Silver están listas para ser unidas entre sí en la capa Gold gracias a las llaves de relación limpias.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##5. Zona GOLD: Construcción del Dataset Integrado de Ventas
-- MAGIC
-- MAGIC En esta sección construiremos el dataset en la capa GOLD, realizando joins entre las tablas de la capa Silver:
-- MAGIC
-- MAGIC - **Ventas** (`silver_fact_delta`)
-- MAGIC - **Empleados** (`silver_empleados_delta`)
-- MAGIC - **Productos** (`silver_producto_delta`)
-- MAGIC - **Locales** (`silver_locales_delta`)
-- MAGIC
-- MAGIC Estos datasets ya fueron limpiados previamente y ahora los relacionaremos para obtener una tabla consolidada de ventas enriquecida.
-- MAGIC

-- COMMAND ----------

-- Crear tabla Gold integrada
CREATE OR REPLACE TABLE gold_ventas
USING DELTA
LOCATION '/FileStore/gold/ventas'
AS
SELECT
    f.dia,
    f.mes,
    f.ano,
    f.sku,
    f.id_vendedor,
    e.sucursal,
    l.sucursal_nombre AS nombre_sucursal,
    l.tipo AS tipo_sucursal,
    p.producto_nombre AS nombre_producto,
    e.nombre_limpio AS nombre_vendedor,
    f.cantidad,
    p.precio_unitario,
    (f.cantidad * p.precio_unitario) AS monto_total
FROM
    silver_fact_delta f
INNER JOIN silver_producto_delta p
    ON f.sku = p.id_producto
INNER JOIN silver_empleados_delta e
    ON f.id_vendedor = e.id_vendedor
INNER JOIN silver_locales_delta l
    ON e.sucursal = l.id_sucursal
WHERE f.mes != 12 -- Ya sabemos que el mes 12 (diciembre) tiene errores
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###5.1. Validaciones

-- COMMAND ----------

SELECT * FROM gold_ventas LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Operaciones avanzadas de MERGE dinámico
-- MAGIC
-- MAGIC En esta sección implementamos una lógica de MERGE dinámico sobre el dataset `silver_empleados_delta`, permitiendo:
-- MAGIC - Insertar nuevos registros (`INSERT`)
-- MAGIC - Actualizar registros existentes (`UPDATE`)
-- MAGIC - Eliminar registros existentes (`DELETE`)
-- MAGIC
-- MAGIC Esto se realiza de manera automática cambiando el parámetro `update_mode`.
-- MAGIC
-- MAGIC **Pasos realizados:**
-- MAGIC 1. Se creó un parámetro `update_mode` con los valores permitidos: 'INSERT', 'UPDATE' o 'DELETE'.
-- MAGIC 2. Se simuló una tabla de staging (`staging_empleados`) para representar nuevos datos.
-- MAGIC 3. Se diseñó una operación MERGE que:
-- MAGIC    - Inserta si no encuentra coincidencias (INSERT).
-- MAGIC    - Actualiza si encuentra coincidencias (UPDATE).
-- MAGIC    - Elimina si encuentra coincidencias (DELETE).
-- MAGIC
-- MAGIC **Justificación:**  
-- MAGIC Esta implementación permite mantener actualizada de manera dinámica la tabla `silver_empleados_delta`, respetando la filosofía de Delta Lake sobre operaciones ACID y control de versiones de los datos.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.1. Staging para INSERTAR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_staging = spark.createDataFrame([
-- MAGIC     (60, 2, "JACKELINE GÓMEZ"),  
-- MAGIC ], ["id_vendedor", "sucursal", "nombre_limpio"])
-- MAGIC
-- MAGIC df_staging.createOrReplaceTempView("staging_empleados")
-- MAGIC
-- MAGIC print("✅ Staging de INSERT listo.")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.2. Staging para ACTUALIZAR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_staging = spark.createDataFrame([
-- MAGIC     (1, 14, "KATHERIN  GÓMEZ"),  # (id_vendedor, sucursal, nombre_limpio)
-- MAGIC ], ["id_vendedor", "sucursal", "nombre_limpio"])
-- MAGIC
-- MAGIC df_staging.createOrReplaceTempView("staging_empleados")
-- MAGIC
-- MAGIC print("✅ Staging de UPDATE listo.")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.3. Staging para ELIMINAR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_staging = spark.createDataFrame([
-- MAGIC     (21, 10, "RAMON PÉREZ"),  # (id_vendedor, sucursal, nombre_limpio)
-- MAGIC ], ["id_vendedor", "sucursal", "nombre_limpio"])
-- MAGIC
-- MAGIC df_staging.createOrReplaceTempView("staging_empleados")
-- MAGIC
-- MAGIC print("✅ Staging de DELETE listo.")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.4. Parámetro update_mode

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Crear parámetro update_mode
-- MAGIC update_mode = 'UPDATE' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.5 Operación dinámica 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if update_mode == 'INSERT':
-- MAGIC     # Verificar si el ID ya existe
-- MAGIC     existing_ids = spark.sql("""
-- MAGIC     SELECT id_vendedor 
-- MAGIC     FROM silver_empleados_delta
-- MAGIC     WHERE id_vendedor IN (SELECT id_vendedor FROM staging_empleados)
-- MAGIC     """).collect()
-- MAGIC
-- MAGIC     if existing_ids:
-- MAGIC         print(f"❗ No se puede insertar. Los siguientes id_vendedor ya existen: {[row['id_vendedor'] for row in existing_ids]}")
-- MAGIC     else:
-- MAGIC         query = """
-- MAGIC         MERGE INTO silver_empleados_delta AS target
-- MAGIC         USING staging_empleados AS source
-- MAGIC         ON target.id_vendedor = source.id_vendedor
-- MAGIC         WHEN NOT MATCHED THEN
-- MAGIC           INSERT (id_vendedor, sucursal, nombre_limpio, fecha_procesamiento)
-- MAGIC           VALUES (source.id_vendedor, source.sucursal, source.nombre_limpio, current_timestamp())
-- MAGIC         """
-- MAGIC         spark.sql(query)
-- MAGIC         print("✅ Inserción exitosa.")
-- MAGIC         
-- MAGIC elif update_mode == 'UPDATE':
-- MAGIC     query = """
-- MAGIC     MERGE INTO silver_empleados_delta AS target
-- MAGIC     USING staging_empleados AS source
-- MAGIC     ON target.id_vendedor = source.id_vendedor
-- MAGIC     WHEN MATCHED THEN
-- MAGIC       UPDATE SET
-- MAGIC         target.nombre_limpio = source.nombre_limpio,
-- MAGIC         target.sucursal = source.sucursal,
-- MAGIC         target.fecha_procesamiento = current_timestamp()
-- MAGIC     """
-- MAGIC     spark.sql(query)
-- MAGIC     print("✅ Actualización exitosa.")
-- MAGIC
-- MAGIC elif update_mode == 'DELETE':
-- MAGIC     query = """
-- MAGIC     MERGE INTO silver_empleados_delta AS target
-- MAGIC     USING staging_empleados AS source
-- MAGIC     ON target.id_vendedor = source.id_vendedor
-- MAGIC     WHEN MATCHED THEN
-- MAGIC       DELETE
-- MAGIC     """
-- MAGIC     spark.sql(query)
-- MAGIC     print("✅ Eliminación exitosa.")
-- MAGIC
-- MAGIC else:
-- MAGIC     raise ValueError("❌ update_mode no válido. Usa 'INSERT', 'UPDATE' o 'DELETE'.")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6.6. Resultado

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Mostrar solo el registro que insertamos
-- MAGIC display(spark.sql("""
-- MAGIC SELECT * 
-- MAGIC FROM silver_empleados_delta
-- MAGIC --WHERE id_vendedor = 21
-- MAGIC --ORDER BY id_vendedor
-- MAGIC """))
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Implementación de una tabla delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7.1. Crear la tabla nueva a partir de silver_empleados_delta

-- COMMAND ----------

CREATE OR REPLACE TABLE final_empleados_delta
USING DELTA
LOCATION '/FileStore/gold/final_empleados_delta'
AS
SELECT *
FROM silver_empleados_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7.2. Validación de tabla creada

-- COMMAND ----------

SELECT * 
FROM final_empleados_delta
ORDER BY id_vendedor;

-- COMMAND ----------

DESCRIBE HISTORY final_empleados_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7.3. Soporte ACID en Delta Lake
-- MAGIC La nueva tabla final_empleados_delta hereda todas las propiedades de Delta Lake, incluyendo:
-- MAGIC
-- MAGIC - Atomicidad (Atomicity): Cada transacción es completa o no ocurre nada. Ejemplo: Si falla una actualización, no queda el dataset a la mitad.
-- MAGIC
-- MAGIC - Consistencia (Consistency): Siempre deja la tabla en un estado válido después de cada operación.
-- MAGIC
-- MAGIC - Aislamiento (Isolation): Cada transacción ocurre sin interferir con otras. No ves cambios de otra transacción hasta que esta se confirme.
-- MAGIC
-- MAGIC - Durabilidad (Durability): Una vez que una operación se completa, sus cambios son permanentes, incluso si ocurre una falla.
-- MAGIC
-- MAGIC **Soporte de Versionamiento (Time Travel)**
-- MAGIC Delta Lake guarda automáticamente las versiones de la tabla. Esto permite:
-- MAGIC
-- MAGIC - Consultar la tabla en un punto anterior en el tiempo.
-- MAGIC
-- MAGIC - Auditar cambios históricos.
-- MAGIC
-- MAGIC - Recuperar datos ante fallos o errores.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Crear una tabla externa apuntando a un directorio de almacenamiento
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Una **tabla externa** en Databricks es una tabla que **no administra directamente el almacenamiento de datos**.  
-- MAGIC En lugar de guardar los datos internamente, **se apunta a un directorio existente** en almacenamiento (como DBFS o ADLS).
-- MAGIC
-- MAGIC **¿Cuándo y por qué usar una tabla externa?**
-- MAGIC
-- MAGIC - Cuando **quieres separar** el control de datos del control de la base de datos.
-- MAGIC - Si el **almacenamiento persiste** aunque la base de datos o el catálogo se eliminen.
-- MAGIC - Para **compartir datos entre clústeres** o **diferentes proyectos** sin duplicar archivos.
-- MAGIC - Para **mayor flexibilidad** en control de versiones y backup de archivos.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###8.1 Creación tabla externa

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_final_empleados = spark.table("final_empleados_delta")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_final_empleados.write.format("delta") \
-- MAGIC   .mode("overwrite") \
-- MAGIC   .option("overwriteSchema", "true") \
-- MAGIC   .save("/FileStore/final/employees_final")
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS external_empleados_final;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS external_empleados_final
USING DELTA
LOCATION '/FileStore/final/employees_final';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###8.2. Validación

-- COMMAND ----------

SELECT * FROM external_empleados_final
ORDER BY id_vendedor;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Crear una vista que calcule 2 KPIs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Con el objetivo de analizar rápidamente información relevante de los empleados procesados, se creó una vista que calcula:
-- MAGIC
-- MAGIC - **Total de empleados** (`total_empleados`): cantidad total de registros únicos.
-- MAGIC - **Promedio de sucursales por empleado** (`promedio_sucursales`): promedio de número de sucursal asignada por empleado.
-- MAGIC
-- MAGIC Esta vista permitirá obtener indicadores globales de calidad y distribución de la información consolidada.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW vista_kpis_empleados AS
SELECT
  COUNT(DISTINCT id_vendedor) AS total_empleados,
  AVG(sucursal) AS promedio_sucursales
FROM final_empleados_delta;


-- COMMAND ----------

SELECT * FROM vista_kpis_empleados;
