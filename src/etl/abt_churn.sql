-- Databricks notebook source
WITH tb_activate AS (

  SELECT idVendedor,
         min(date(dtPedido)) as dtAtivacao

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido >= '2018-01-01'
  AND t1.dtPedido <= date_add('2018-01-01', 45)
  AND idVendedor IS NOT NULL

  GROUP BY 1

)

SELECT t1.*,
       t2.*,
       t3.*,
       t4.*,
       t5.*,
       t6.*,
       case when t7.idVendedor IS NULL THEN 1 ELSE 0 END AS flChurn

FROM silver.analytics.fs_vendedor_vendas AS t1

LEFT JOIN silver.analytics.fs_vendedor_avaliacao AS t2
ON t1.idVendedor = t2.idVendedor
AND t1.dtReference = t2.dtReference

LEFT JOIN silver.analytics.fs_vendedor_cliente AS t3
ON t1.idVendedor = t3.idVendedor
AND t1.dtReference = t3.dtReference

LEFT JOIN silver.analytics.fs_vendedor_entrega AS t4
ON t1.idVendedor = t4.idVendedor
AND t1.dtReference = t4.dtReference

LEFT JOIN silver.analytics.fs_vendedor_pagamentos AS t5
ON t1.idVendedor = t5.idVendedor
AND t1.dtReference = t5.dtReference

LEFT JOIN silver.analytics.fs_vendedor_produto AS t6
ON t1.idVendedor = t6.idVendedor
AND t1.dtReference = t6.dtReference

LEFT JOIN tb_activate AS t7
ON t1.idVendedor = t7.idVendedor
AND datediff(t7.dtAtivacao, t1.dtReference) + t1.qtdRecencia <= 45

WHERE t1.qtdRecencia <= 45
