-- Databricks notebook source
WITH tb_pedido AS (

SELECT t1.idPedido,
       t2.idVendedor,
       t1.descSituacao,
       t1.dtPedido,
       t1.dtAprovado,
       t1.dtEntregue,
       t1.dtEstimativaEntrega,
       sum(vlFrete) as totalFrente       

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido as t2
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
    COUNT(DISTINCT CASE WHEN date(coalesce(dtEntregue, '2018-01-01')) > date(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
    count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) AS pctPedidoCancelado,
    avg(totalFrente) as avgFrete,
    percentile(totalFrente, 0.5) as medianFrete,
    max(totalFrente) as maxFrete,
    min(totalFrente) as minFrete,
    avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
    avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
    avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) AS qtdeDiasEntregaPromessa
       
FROM tb_pedido

GROUP BY 1
