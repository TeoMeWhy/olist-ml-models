-- Databricks notebook source
SELECT *
FROM silver.olist.pagamento_pedido
TABLESAMPLE(5 ROWS)

-- COMMAND ----------

SELECT DATE(dtPedido) AS dtPedido,
  COUNT(*) AS qtPedido
FROM silver.olist.pedido 
WHERE dtPedido < '2018-01-01'
  AND dtPedido >= ADD_MONTHS('2018-01-01', -6)
GROUP BY 1
ORDER BY 1


-- COMMAND ----------

WITH base_geral AS (
SELECT a.*,
  c.idVendedor

FROM silver.olist.pagamento_pedido AS a

LEFT JOIN silver.olist.pedido AS b
  ON a.idPedido = b.idPedido  

LEFT JOIN silver.olist.item_pedido AS c
  ON a.idPedido = c.idPedido

WHERE c.idVendedor IS NOT NULL
  AND b.dtPedido < '2018-01-01'
  AND b.dtPedido >= ADD_MONTHS('2018-01-01', -6)),

base_agrupada AS (
SELECT idVendedor,
  descTipoPagamento,
  COUNT(DISTINCT idPedido) AS qtd_vendas,
  SUM(vlPagamento) AS valor_total

FROM base_geral

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor)

SELECT idVendedor,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN valor_total ELSE 0 END) AS valor_vendas_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_credit_card,

  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_debit_card,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN valor_total ELSE 0 END) AS valor_vendas_debit_card,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_debit_card,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_boleto,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN valor_total ELSE 0 END) AS valor_vendas_boleto,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_boleto,

  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_voucher,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN valor_total ELSE 0 END) AS valor_vendas_voucher,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_voucher

FROM base_agrupada

GROUP BY idVendedor

-- COMMAND ----------


