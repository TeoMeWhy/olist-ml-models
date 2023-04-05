-- Databricks notebook source
SELECT date(dtPedido) as dtPedido,
        count(*) as qtPedido

FROM silver.olist.pedido

GROUP BY 1
ORDER BY 1

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido 

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01',-6)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Pagamentos dos pedidos dos ultimos seis meses

-- COMMAND ----------

SELECT t2.*

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01',-6)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Pagamentos dos pedidos dos ultimos seis meses com info do vendedor

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido AS t1

-- COMMAND ----------

WITH tb_join AS (

  SELECT t2.*,
         t3.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido AS t3
  ON t1.idPedido = t3.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t3.idVendedor IS NOT NULL

),

tb_group AS (

  SELECT idVendedor,
         descTipoPagamento,
         count(distinct idPedido) as qtdePedidoMeioPagamento,
         sum(vlPagamento) as vlPedidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento

)

SELECT 
  idVendedor,

  sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
  sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
  sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
  sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

  sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
  sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
  sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
  sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,
  
  sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
  sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
  sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
  sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

  sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
  sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
  sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
  sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

FROM tb_group

GROUP BY 1
