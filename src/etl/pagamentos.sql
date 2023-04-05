-- Databricks notebook source
SELECT 

-- COMMAND ----------

SELECT date(dtPedido) as dtPedido,
        count(*) as qtPedido

FROM silver.olist.pedido

GROUP BY 1
ORDER BY 1

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido 

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01',-6)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Pagamentos dos pedidos dos ultimos seis meses

-- COMMAND ----------

SELECT t2.*

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01',-6)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Pagamentos dos pedidos dos ultimos seis meses com info do vendedor

-- COMMAND ----------

WITH tb_join AS (

  SELECT t2.*,
         t3



)
