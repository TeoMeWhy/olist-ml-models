-- Databricks notebook source
WITH base AS (
  SELECT 
    pag.*,
    item.idVendedor
  FROM silver.olist.pedido as ped
  LEFT JOIN silver.olist.pagamento_pedido as pag ON ped.idPedido = pag.idPedido
  LEFT JOIN silver.olist.item_pedido as item ON item.idPedido = pag.idPedido
  WHERE
    ped.dtPedido >= add_months('2018-01-01', -6) AND ped.dtPedido < '2018-01-01' AND
    item.idVendedor IS NOT NULL
), groupBase AS (
  SELECT
    idVendedor,
    descTipoPagamento,
    COUNT(DISTINCT idPedido) as qtd_Pedido_MeioPagamento,
    SUM(vlPagamento) as vl_Pagamento_MeioPagamento
  FROM base
  GROUP BY 1, 2
  ORDER BY 1, 2
), summaryBase AS (
  SELECT
    idVendedor,
    -- Quantidade de Pedidos por Vendedor e Meio de Pagamento
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_Pedido_MeioPagamento ELSE 0 END) AS qtd_boleto,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_Pedido_MeioPagamento ELSE 0 END) AS qtd_credit_card,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_Pedido_MeioPagamento ELSE 0 END) AS qtd_voucher,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_Pedido_MeioPagamento ELSE 0 END) AS qtd_debit_card,
    -- Distribuição percentual de Pedidos por Vendedor e Meio de Pagamento
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_Pedido_MeioPagamento ELSE 0 END) / SUM(qtd_Pedido_MeioPagamento) AS per_qtd_boleto,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_Pedido_MeioPagamento ELSE 0 END) / SUM(qtd_Pedido_MeioPagamento) AS per_qtd_credit_card,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_Pedido_MeioPagamento ELSE 0 END) / SUM(qtd_Pedido_MeioPagamento) AS per_qtd_voucher,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_Pedido_MeioPagamento ELSE 0 END) / SUM(qtd_Pedido_MeioPagamento) AS per_qtd_debit_card,
    -- Soma de valor pago por Vendedor e Meio de Pagamento
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vl_Pagamento_MeioPagamento ELSE 0 END) AS vl_boleto,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vl_Pagamento_MeioPagamento ELSE 0 END) AS vl_credit_card,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vl_Pagamento_MeioPagamento ELSE 0 END) AS vl_voucher,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vl_Pagamento_MeioPagamento ELSE 0 END) AS vl_debit_card,
    -- Distribuição percentual da soma de valor pago por Vendedor e Meio de Pagamento
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vl_Pagamento_MeioPagamento ELSE 0 END) / SUM(vl_Pagamento_MeioPagamento) AS per_vl_boleto,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vl_Pagamento_MeioPagamento ELSE 0 END) / SUM(vl_Pagamento_MeioPagamento) AS per_vl_credit_card,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vl_Pagamento_MeioPagamento ELSE 0 END) / SUM(vl_Pagamento_MeioPagamento) AS per_vl_voucher,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vl_Pagamento_MeioPagamento ELSE 0 END) / SUM(vl_Pagamento_MeioPagamento) AS per_vl_debit_card  
  FROM groupBase
  GROUP BY 1
  ORDER BY 1
), cardInstallmentsBase AS (
  SELECT
    idVendedor,
    AVG(nrParcelas) AS avg_qtd_nrParcelas,
    PERCENTILE(nrParcelas, 0.5) AS median_qtd_nrParcelas,
    MIN(nrParcelas) AS min_qtd_nrParcelas,
    MAX(nrParcelas) AS max_qtd_nrParcelas
  FROM base
  WHERE
    descTipoPagamento = 'credit_card'
  GROUP BY 1
  ORDER BY 1
)
SELECT
  '2018-01-01' AS dt_reference,
  t1.*,
  t2.avg_qtd_nrParcelas,
  t2.median_qtd_nrParcelas,
  t2.min_qtd_nrParcelas,
  t2.max_qtd_nrParcelas
FROM summaryBase AS t1
LEFT JOIN cardInstallmentsBase AS t2 ON t2.idVendedor = t1.idVendedor
;
