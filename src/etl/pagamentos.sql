-- Databricks notebook source
SELECT DATE(dtPedido) as dtPedido,
    count(*) as qtPedido
FROM silver.olist.pedido
group by dtPedido
order by 1

-- COMMAND ----------

WITH tb_join AS (
  SELECT t2.*,
          t3.idVendedor
  FROM silver.olist.pedido AS t1
  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.item_pedido AS t3
  ON t1.idPedido = t3.idPedido
  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND t3.idVendedor IS NOT NULL
),

tb_group AS (
  SELECT idVendedor,
      descTipoPagamento,
      COUNT(DISTINCT idPedido) as qtdPedidoMeioPagamento,
      SUM(vlPagamento) as vlPedidoMeioPagamento
  FROM tb_join
  GROUP BY idVendedor, descTipoPagamento
)

SELECT idVendedor,
         SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtdBoletoPedido,
         SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtdCreditCardPedido,
        SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtdVoucherPedido,
        SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtdDebitCardPedido,
        SUM(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
        sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
        sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
        sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

        sum(case when descTipoPagamento='boleto' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as pct_qtd_boleto_pedido,
        sum(case when descTipoPagamento='credit_card' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as pct_qtd_credit_card_pedido,
        sum(case when descTipoPagamento='voucher' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as pct_qtd_voucher_pedido,
        sum(case when descTipoPagamento='debit_card' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as pct_qtd_debit_card_pedido,

        sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
        sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
        sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
        sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido
FROM tb_group
GROUP BY idVendedor

-- COMMAND ----------


