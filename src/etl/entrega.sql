-- Databricks notebook source
WITH tb_pedido AS (
SELECT
  t1.idPedido,
  t2.idVendedor,
  t1.descSituacao,
  t1.dtPedido,
  t1.dtAprovado,
  t1.dtEntregue,
  t1.dtEstimativaEntrega,
  SUM(vlFrete) AS totalFrete

FROM silver.olist.pedido t1

LEFT JOIN silver.olist.item_pedido t2
ON t1.idPedido = t2.idPedido

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)

GROUP BY
  t1.idPedido,
  t2.idVendedor,
  t1.descSituacao,
  t1.dtPedido,
  t1.dtAprovado,
  t1.dtEntregue,
  t1.dtEstimativaEntrega
  )
  
SELECT 
  idVendedor,
  
  COUNT(CASE WHEN DATE(COALESCE(dtEntregue, '2018-01-01')) > DATE(dtEstimativaEntrega)
    AND DATE(dtEntregue) > DATE(dtEstimativaEntrega) THEN idPedido END) /
  COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
  
    COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
    
    AVG(totalFrete) AS avgFrete,
    
    PERCENTILE(totalFrete, 0.5) AS medianFrete,
    
    MAX(totalFrete) AS maxFrete,
    
    MIN(totalFrete) AS minFrete,
    
    AVG(datediff(COALESCE(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
    
    AVG(datediff(COALESCE(dtEntregue, '2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
    
    AVG(datediff(dtEstimativaEntrega, COALESCE(dtEntregue, '2018-01-01'))) AS qtdeDiasEntregaPromessa

FROM tb_pedido

GROUP BY 1

-- COMMAND ----------


