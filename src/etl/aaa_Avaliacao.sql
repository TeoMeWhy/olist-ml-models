-- Databricks notebook source
-- MAGIC %md Issue
-- MAGIC 
-- MAGIC - Avaliação média em NPS
-- MAGIC - Nota média de avaliação
-- MAGIC - Quantidade de avaliações
-- MAGIC - Quantidade de avaliações negativas - não feita

-- COMMAND ----------

WITH tb_pedido AS (

SELECT DISTINCT 
       t1.idPedido,
       t2.idVendedor 
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido


WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)
AND idVendedor IS NOT NULL),

tb_join AS (

SELECT t1.*,
       t2.vlNota

FROM tb_pedido AS t1

LEFT JOIN silver.olist.avaliacao_pedido AS t2
ON t1.idPedido = t2.idPedido),

tb_summary AS (

SELECT idVendedor,
       AVG(vlNota) as avgNota,
       percentile(vlNota, 0.5) as medianNota,
       min(vlNota) as minNota,
       max(vlNota) as maxNota,
       count(vlNota) / count(idPedido) as pctAvaliacao

FROM tb_join

GROUP BY 1)

SELECT '2018-01-01' dtReference,* 
FROM tb_summary

-- COMMAND ----------


