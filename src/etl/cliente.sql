-- Databricks notebook source
with tb_join as (
select distinct t1.idPedido,
t1.idCliente,
t2.idVendedor,
t3.descUF
from silver.olist.pedido as t1
left join silver.olist.item_pedido as t2
on t1.idPedido = t2.idPedido
left join silver.olist.cliente as t3
on t1.idCliente = t3.idCliente
where dtPedido <= '2018-01-01' and dtpedido >= add_months('2018-01-01', -6)
)
select * from tb_join

-- COMMAND ----------


