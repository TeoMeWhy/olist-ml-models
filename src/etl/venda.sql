-- Databricks notebook source
WITH tb_pedido_item AS (
SELECT
  t2.*,
  t1.dtPedido
  
FROM silver.olist.pedido t1

LEFT JOIN silver.olist.item_pedido t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL
),

tb_summary AS (
SELECT 
  idVendedor,
  COUNT(DISTINCT idPedido) AS qtdPedidos,
  COUNT(DISTINCT date(dtPedido)) AS qtdDias,
  COUNT(idProduto) AS qtdItens,
  datediff('2018-01-01', MAX(dtPedido)) AS qtdRecencia,
  SUM(vlPreco)/COUNT(DISTINCT idPedido) AS avgTicket,
  AVG(vlPreco) AS avgValorProduto,
  MAX(vlPreco) AS maxValorProduto,
  MIN(vlPreco) AS minValorProduto,
  COUNT(idProduto)/COUNT(DISTINCT idPedido) AS avgProdutoPedido
  
FROM tb_pedido_item

GROUP BY idVendedor
),

tb_pedido_summary AS(
SELECT 
  idVendedor,
  idPedido,
  SUM(vlPreco) AS vlPreco
  
FROM tb_pedido_item

GROUP BY idVendedor, idPedido
),

tb_min_max AS (
SELECT
  idVendedor,
  MIN(vlPreco) AS minVlPedido,
  MAX(vlPreco) AS maxVlPedido

FROM tb_pedido_summary

GROUP BY idVendedor
),

tb_life AS (
SELECT
  t2.idVendedor,
  SUM(vlPreco) AS LTV,
  MAX(datediff('2018-01-01', dtPedido)) AS qtdDiasBase
  
FROM silver.olist.pedido t1

LEFT JOIN silver.olist.item_pedido t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t2.idVendedor IS NOT NULL

GROUP BY t2.idVendedor
),

tb_dtpedido AS (
SELECT 
  DISTINCT idVendedor,
  DATE(dtPedido) AS dtPedido

FROM tb_pedido_item

GROUP BY 1,2
),

tb_lag AS (
SELECT 
  *,
  LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1
  
FROM tb_dtpedido
),

tb_intervalo AS (
SELECT 
  idVendedor,
  AVG(datediff(dtPedido, lag1)) AS avgIntervaloVendas

FROM tb_lag

GROUP BY idVendedor
)

SELECT 
  '2018-01-01' AS dtReference,
  t1.*,
  t2.minVlPedido,
  t2.maxVlPedido,
  t3.LTV,
  t3.qtdDiasBase,
  t4.avgIntervaloVendas

FROM tb_summary AS t1

LEFT JOIN tb_min_max AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life AS t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo AS t4
ON t1.idVendedor = t4.idVendedor

-- COMMAND ----------


