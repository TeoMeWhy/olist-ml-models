-- Databricks notebook source
WITH tb_join AS (
SELECT t2.*,
       t3.idVendedor
       
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido as t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.item_pedido as t3
ON t1.idPedido = t3.idPedido


WHERE dtPedido < '2018-01-01'
AND dtPedido >=  add_months ('2018-01-01',-6)
AND t3.idVendedor IS NOT NULL
),

tb_group AS (
SELECT idVendedor,
       descTipoPagamento,
       count(distinct idPedido) as qtPedidoMeioPagamento,
       sum (vlPagamento) as vlPedidoMeioPagamento
       
FROM tb_join

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento
)


SELECT idVendedor,

-- Pivot calculando as quantidades

sum(CASE WHEN descTipoPagamento = 'boleto' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,

sum(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,

-- Para calcular proporção 

sum(CASE WHEN descTipoPagamento = 'boleto' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_debit_card_pedido,

sum(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

FROM tb_group

GROUP BY 1
