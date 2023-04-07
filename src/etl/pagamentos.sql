-- Databricks notebook source
WITH 
tb_pedidos AS (
  SELECT
    DISTINCT t1.idPedido,
    t2.idVendedor
    
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido
  
  WHERE t1.dtPedido BETWEEN '2017-07-01' AND '2018-01-01'
  AND t2.idVendedor IS NOT NULL
),

tb_join AS (
  SELECT
    t1.idVendedor,
    t2.*
    
  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido
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
),

tb_summary(
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
    qtde_boleto_pedido / SUM(qtdePedidoMeioPagamento) AS pct_qtde_boleto_pedido,
    qtde_credit_card_pedido / SUM(qtdePedidoMeioPagamento) AS pct_qtde_credit_card_pedido,
    qtde_voucher_pedido / SUM(qtdePedidoMeioPagamento) AS pct_qtde_voucher_pedido,
    qtde_debit_card_pedido / SUM(qtdePedidoMeioPagamento) AS pct_qtde_debit_card_pedido,

    -- Proporcao de valor pago por tipo de pagamento
    valor_boleto_pedido / SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
    valor_credit_card_pedido / SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
    valor_voucher_pedido / SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
    valor_debit_card_pedido / SUM(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

  FROM tb_group
  GROUP BY 1
),

tb_cartao AS (
  SELECT
    idVendedor,
    AVG(nrParcelas) AS avgQtdeParcelas,
    PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
    MAX(nrParcelas) AS maxQtdeParcelas,
    MIN(nrParcelas) AS minQtdeParcelas

  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor
)

SELECT 
  '2018-01-01' AS dtReference,
  t1.*,
  t2.avgQtdeParcelas,
  t2.medianQtdeParcelas,
  t2.maxQtdeParcelas,
  t2.minQtdeParcelas
  
FROM tb_summary as t1

LEFT JOIN tb_cartao AS t2
ON t1.idVendedor = t2.idVendedor
