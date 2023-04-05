-- Databricks notebook source
-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t1.idPedido,
         t1.idCliente,
         t2.idVendedor,
         t3.descUF

  FROM silver.olist.pedido AS  t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente as t3
  ON t1.idCliente = t3.idCliente

  WHERE dtPedido < '2018-01-01'
  and dtPedido >= add_months('2018-01-01', -6)

)

SELECT * FROM tb_join
