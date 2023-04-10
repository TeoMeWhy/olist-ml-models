-- Databricks notebook source
-- MAGIC %md # Issues 
-- MAGIC 
-- MAGIC - % de entregas com atraso
-- MAGIC - Valor de frete (média ou mediana)
-- MAGIC - Tempo médio de entrega
-- MAGIC - Pedidos cancelados
-- MAGIC - Diferença entre promessa e entrega (em dias)

-- COMMAND ----------

WITH tb_pedido AS (

SELECT t1.idPedido,
       t2.idVendedor,
       t1.descSituacao,
       t1.dtPedido,
       t1.dtAprovado,
       t1.dtEntregue,
       t1.dtEstimativaEntrega,
       sum(vlFrete) as TotalFrete
     

FROM silver.olist.pedido t1

LEFT JOIN silver.olist.item_pedido t2
ON t1.idPedido = t2.idPedido

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)

GROUP BY t1.idPedido,
       t2.idVendedor,
       t1.descSituacao,
       t1.dtPedido,
       t1.dtAprovado,
       t1.dtEntregue,
       t1.dtEstimativaEntrega

)

SELECT 
       idVendedor,
       COUNT( DISTINCT CASE WHEN DATE(coalesce(dtEntregue, '2018-01-01')) > DATE(dtEstimativaEntrega) THEN idPedido END) /
       COUNT( DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
       COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidosCancelado,
       AVG(totalFrete) as avgFrete,
       PERCENTILE(totalFrete, 0.5) AS medianFrete,
       MAX(totalFrete) as maxFrete,
       MIN(totalFrete) as minFrete,
       AVG(DATEDIFF(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
       AVG(DATEDIFF(coalesce(dtEntregue, '2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
       AVG(DATEDIFF(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) AS qtdDiasEntregaPromessa
       
     
FROM tb_pedido

GROUP BY 1




-- COMMAND ----------


