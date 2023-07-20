-- Databricks notebook source
SELECT 
*
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
