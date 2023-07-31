-- Databricks notebook source
CREATE TABLE sandbox.analystics_churn_model.fs_vendedor_entrega AS

WITH tbl_pedido AS (
SELECT 
  t1.idPedido, 
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,  
  t1.dtEnvio, 
  t1.dtEstimativaEntrega,
  t1.dtEntregue, 
  SUM(t2.vlFrete) AS TotalFrete 
FROM 
  silver.olist.pedido t1 
LEFT JOIN 
  silver.olist.item_pedido t2 
    ON t1.idPedido = t2.idPedido 
WHERE 
  dtPedido >= ADD_MONTHS('2018-01-01', -6)
AND 
  dtPedido < '2018-01-01'
AND 
  t2.idVendedor IS NOT NULL
GROUP BY 
  t1.idPedido, 
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,  
  t1.dtEnvio, 
  t1.dtEstimativaEntrega,
  t1.dtEntregue) 

SELECT 
    '2018-01-01' as DateRef, 
    idVendedor,
    COUNT(DISTINCT CASE WHEN  descSituacao  = 'canceled' THEN idPedido end) / COUNT (DISTINCT idPedido) AS pctPedidoCancelado, 
    COUNT(DISTINCT CASE WHEN  DATE(COALESCE(dtEntregue,'2018-01-01')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao  = 'delivered' THEN idPedido END) AS pctPedidoEntregueAtrasado, 
    AVG(TotalFrete) AS AvgFrete,
    PERCENTILE(TotalFrete, 0.50) AS MedianFrete, 
    MAX(TotalFrete) AS MaxFrete, 
    MIN(TotalFrete) AS MinFrete,
    AVG(DATEDIFF(COALESCE(dtEntregue,'2018-01-01'), dtAprovado)) AS AvgDiasAprovadoEntrega,
    AVG(DATEDIFF(COALESCE(dtEntregue,'2018-01-01'),dtPedido)) AS AvgDiasPedidoEntrega, 
    AVG(DATEDIFF(dtEstimativaEntrega,COALESCE(dtEntregue,'2018-01-01'))) AS qntDiasEntregaPromessa
FROM 
  tbl_pedido
GROUP BY 
  idVendedor
