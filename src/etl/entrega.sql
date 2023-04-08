-- Databricks notebook source
WITH tb_pedido AS (
  SELECT t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega,
         SUM(vlFrete) AS totalFrete


  FROM silver.olist.pedido AS t1
  JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)

  GROUP BY t1.idPedido,
          t2.idVendedor,
          t1.descSituacao,
          t1.dtPedido,
          t1.dtAprovado,
          t1.dtEntregue,
          t1.dtEstimativaEntrega
)


SELECT  idVendedor,
      COUNT(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue, '2018-01-01')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pcPedidoAtrasado,
      COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
      AVG(totalFrete) AS avgFrente,
      PERCENTILE(totalFrete, 0.5) AS medianFrente,
      MAX(totalFrete) AS maxFrente,
      MIN(totalFrete) AS minFrente,
      AVG(DATEDIFF(COALESCE(dtEntregue,'2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
      AVG(DATEDIFF(COALESCE(dtEntregue,'2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
      AVG(DATEDIFF(dtEstimativaEntrega, COALESCE(dtEntregue,'2018-01-01'))) AS qtdDiasPromessaEntrega
FROM tb_pedido
GROUP BY 1

-- COMMAND ----------


