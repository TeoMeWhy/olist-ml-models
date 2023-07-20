-- Databricks notebook source


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
  t1.dtEntregue
