-- Databricks notebook source
WITH tb_join AS (

SELECT t2.*
, t3.idVendedor
  FROM silver.olist.pedido as t1
  LEFT JOIN silver.olist.pagamento_pedido as t2
  ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.item_pedido as t3
  ON t1.idPedido = t3.idPedido
    WHERE dtPedido < '2018-01-01'
    AND dtPedido >= add_months('2018-01-01', -6)
    AND t3.idVendedor IS NOT NULL
    
    
),

tb_group AS (

SELECT idVendedor
, descTipoPagamento
, count(distinct idPedido) as qtdePedidoMeioPagamento
, sum(vlPagamento) as vlPedidoMeioPagamento
  FROM tb_join
    GROUP BY idVendedor
    , descTipoPagamento
      ORDER BY idVendedor
      , descTipoPagamento
      
)

SELECT idVendedor

, sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido
, sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido
, sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido
, sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido

, sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido
, sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido
, sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido
, sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido

, sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_boleto_pedido
, sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_credit_card_pedido
, sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_voucher_pedido
, sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_debit_card_pedido

, sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valo_boleto_pedido
, sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valo_credit_card_pedido
, sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valo_voucher_pedido
, sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valo_debit_card_pedido


FROM tb_group
  GROUP BY 1

-- COMMAND ----------


