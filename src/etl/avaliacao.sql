-- Databricks notebook source
WITH tb_pedido AS (
  SELECT DISTINCT
        t1.idPedido,
        t2.idVendedor
  FROM silver.olist.pedido AS t1
  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL
),
tb_join as (
  SELECT t1.*, 
          t2.vlNota
  FROM tb_pedido AS t1
  LEFT JOIN silver.olist.avaliacao_pedido AS t2
  ON t1.idPedido = t2.idPedido
),
tb_summary AS (
  SELECT idVendedor,
          AVG(vlNota) AS avgNota,
          PERCENTILE(vlNota, 0.5) AS avgNota,
          MIN(vlNota) AS minNota,
          MAX(vlNota) AS maxNota,
          COUNT(vlNota) / COUNT(idPedido) as pctAvaliacao
  FROM tb_join
  GROUP BY idVendedor
)
SELECT '2018-01-01' AS dtReference,
        *
FROM tb_summary

-- COMMAND ----------


