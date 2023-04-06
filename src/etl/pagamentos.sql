-- Databricks notebook source
WITH 

tb_join AS (
  SELECT
    t2.*,
    t3.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido AS t3
  ON t1.idPedido = t3.idPedido

  WHERE t1.dtPedido BETWEEN '2017-07-01' AND '2018-01-01'
  AND t3.idVendedor IS NOT NULL
),

tb_group AS (
  SELECT
  idVendedor,
  descTipoPagamento,
  COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
  SUM(vlPagamento) AS vlPedidoMeioPagamento
  
  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
)

SELECT 
  idVendedor,
  
  -- Quantidade de pedidos de acordo com pagamento
  SUM(
    CASE WHEN descTipoPagamento = 'boleto'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) AS qtde_boleto_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'credit_card'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) AS qtde_credit_card_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'voucher'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) AS qtde_voucher_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'debit_card'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) AS qtde_debit_card_pedido,
  
  -- Total pago por tipo de pagamento
  SUM(
    CASE WHEN descTipoPagamento = 'boleto'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) AS valor_boleto_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'credit_card'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) AS valor_credit_card_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'voucher'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) AS valor_voucher_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'debit_card'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) AS valor_debit_card_pedido,

-- Proporcao de pedidos de acordo com tipo de pagamento
  SUM(
    CASE WHEN descTipoPagamento = 'boleto'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_boleto_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'credit_card'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_credit_card_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'voucher'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_voucher_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'debit_card'
    THEN qtdePedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(qtdePedidoMeioPagamento) AS pct_qtd_debit_card_pedido,
  
  -- Proporcao de valor pago por tipo de pagamento
  SUM(
    CASE WHEN descTipoPagamento = 'boleto'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'credit_card'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'voucher'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
  SUM(
    CASE WHEN descTipoPagamento = 'debit_card'
    THEN vlPedidoMeioPagamento 
    ELSE 0 END
  ) / SUM(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

FROM tb_group
GROUP BY 1

-- SELECT * FROM tb_group

