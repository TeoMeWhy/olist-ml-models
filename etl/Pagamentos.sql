-- Databricks notebook source
SELECT * FROM silver.olist.pagamento_pedido

-- COMMAND ----------

WITH tb_join AS (
SELECT t2.* ,t3.idVendedor

FROM silver.olist.pedido as t1
LEFT JOIN silver.olist.pagamento_pedido as t2
ON t1.idPedido=t2.idPedido
LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido=t3.idPedido
WHERE dtPedido < '2018-01-01' AND dtPedido > add_months('2018-01-01',-6)),

tb_group AS (

SELECT idVendedor,
      descTipoPagamento,
      count(distinct idPedido) as qtdPedidosMeioPagamento,
      sum(vlPagamento) as vlPagamentoMeioPagamento
FROM tb_join
GROUP BY idVendedor,descTipoPagamento
ORDER BY idVendedor,descTipoPagamento)

SELECT 
idVendedor,
SUM(CASE WHEN descTipoPagamento= 'boleto' THEN qtdPedidosMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
SUM(CASE WHEN descTipoPagamento= 'credit_card' THEN qtdPedidosMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento= 'voucher' THEN qtdPedidosMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,
SUM(CASE WHEN descTipoPagamento= 'debit_card' THEN qtdPedidosMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,

SUM(CASE WHEN descTipoPagamento= 'boleto' THEN vlPagamentoMeioPagamento ELSE 0 END) AS vlPagamento_boleto_pedido,
SUM(CASE WHEN descTipoPagamento= 'credit_card' THEN vlPagamentoMeioPagamento ELSE 0 END) AS vlPagamento_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento= 'voucher' THEN vlPagamentoMeioPagamento ELSE 0 END) AS vlPagamento_voucher_pedido,
SUM(CASE WHEN descTipoPagamento= 'debit_card' THEN vlPagamentoMeioPagamento ELSE 0 END) AS vlPagamento_debit_card_pedido,

SUM(CASE WHEN descTipoPagamento= 'boleto' THEN qtdPedidosMeioPagamento ELSE 0 END)/SUM(qtdPedidosMeioPagamento) AS pct_qtde_boleto_pedido,
SUM(CASE WHEN descTipoPagamento= 'credit_card' THEN qtdPedidosMeioPagamento ELSE 0 END)/SUM(qtdPedidosMeioPagamento) AS pct_qtde_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento= 'voucher' THEN qtdPedidosMeioPagamento ELSE 0 END)/SUM(qtdPedidosMeioPagamento) AS pct_qtde_voucher_pedido,
SUM(CASE WHEN descTipoPagamento= 'debit_card' THEN qtdPedidosMeioPagamento ELSE 0 END)/SUM(qtdPedidosMeioPagamento) AS pct_qtde_debit_card_pedido,

SUM(CASE WHEN descTipoPagamento= 'boleto' THEN vlPagamentoMeioPagamento ELSE 0 END)/SUM (vlPagamentoMeioPagamento) AS pct_vlPagamento_boleto_pedido,
SUM(CASE WHEN descTipoPagamento= 'credit_card' THEN vlPagamentoMeioPagamento ELSE 0 END)/SUM (vlPagamentoMeioPagamento) AS pct_vlPagamento_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento= 'voucher' THEN vlPagamentoMeioPagamento ELSE 0 END)/SUM (vlPagamentoMeioPagamento) AS pct_vlPagamento_voucher_pedido,
SUM(CASE WHEN descTipoPagamento= 'debit_card' THEN vlPagamentoMeioPagamento ELSE 0 END)/SUM (vlPagamentoMeioPagamento) AS pct_vlPagamento_debit_card_pedido


from tb_group
GROUP BY 1

-- COMMAND ----------


