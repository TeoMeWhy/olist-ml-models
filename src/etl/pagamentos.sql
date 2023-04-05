-- Databricks notebook source
SELECT date(dtPedido) as dtPedido,
        count(*) as qtPedido
        
FROM silver.olist.pedido

GROUP BY 1
ORDER BY 1

-- COMMAND ----------



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

  sum(CASE WHEN descTipoPagamento='boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
  sum(CASE WHEN descTipoPagamento='credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
  sum(CASE WHEN descTipoPagamento='voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,
  sum(CASE WHEN descTipoPagamento='debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,

  sum(CASE WHEN descTipoPagamento='boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
  sum(CASE WHEN descTipoPagamento='credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
  sum(CASE WHEN descTipoPagamento='voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
  sum(CASE WHEN descTipoPagamento='debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,
  
  sum(CASE WHEN descTipoPagamento='boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_boleto_pedido,
  sum(CASE WHEN descTipoPagamento='credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_credit_card_pedido,
  sum(CASE WHEN descTipoPagamento='voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_voucher_pedido,
  sum(CASE WHEN descTipoPagamento='debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_debit_card_pedido,

  sum(CASE WHEN descTipoPagamento='boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
  sum(CASE WHEN descTipoPagamento='credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
  sum(CASE WHEN descTipoPagamento='voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
  sum(CASE WHEN descTipoPagamento='debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

FROM tb_group

GROUP BY 1

-- COMMAND ----------


