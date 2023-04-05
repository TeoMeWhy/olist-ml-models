-- Databricks notebook source
SELECT * 
FROM silver.olist.pagamento_pedido

-- COMMAND ----------

SELECT date(dtPedido) as dtPedido,
       count(*) as qtPedido
FROM silver.olist.pedido
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH tb_join (

  SELECT t2.*,
         t3.idVendedor
  FROM silver.olist.pedido as t1

  LEFT JOIN silver.olist.pagamento_pedido as t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido as t3
  ON t1.idPedido = t3.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t3.idVendedor IS NOT NULL
),

tb_group AS (

  SELECT idVendedor, 
         descTipoPagamento, 
         count(distinct idPedido) as qntPedidoMeioPagamento,
         sum(vlPagamento) as vlPedidoMeioPagamento
  FROM tb_join
  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
)

SELECT idVendedor,
  SUM(CASE WHEN descTipoPagamento = 'boleto' then qntPedidoMeioPagamento else 0 end) AS qnt_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' then qntPedidoMeioPagamento else 0 end) AS qnt_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' then qntPedidoMeioPagamento else 0 end) AS qnt_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' then qntPedidoMeioPagamento else 0 end) AS qnt_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) AS valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) AS valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) AS valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) AS valor_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento='boleto' then qntPedidoMeioPagamento else 0 end) / sum(qntPedidoMeioPagamento) as pct_qtd_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento='credit_card' then qntPedidoMeioPagamento else 0 end) / sum(qntPedidoMeioPagamento) as pct_qtd_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento='voucher' then qntPedidoMeioPagamento else 0 end) / sum(qntPedidoMeioPagamento) as pct_qtd_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento='debit_card' then qntPedidoMeioPagamento else 0 end) / sum(qntPedidoMeioPagamento) as pct_qtd_debit_card_pedido,

  SUM(CASE WHEN descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

FROM tb_group
GROUP BY 1

-- COMMAND ----------


