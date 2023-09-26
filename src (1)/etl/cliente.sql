-- Databricks notebook source
WITH base_geral AS ( 
SELECT DISTINCT a.idPedido,
a.idCliente,
b.idVendedor,
c.descUF

FROM silver.olist.pedido AS a

LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

LEFT JOIN silver.olist.cliente AS c
    ON a.idCliente = c.idCliente

WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6 ))

