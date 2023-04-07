-- Databricks notebook source
WITH tb_join AS(

SELECT 
DISTINCT t1.idPedido,
         t1.idCliente,
         t2.idvendedor,
         t3.descUF
      
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
USING(idPedido)
LEFT JOIN silver.olist.cliente as t3
USING (idCliente)


WHERE dtPedido < '2018-01-01' AND dtPedido > add_months('2018-01-01',-6)
AND idVendedor is not null)

select * from TB_JOIN

-- COMMAND ----------


