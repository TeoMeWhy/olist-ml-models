-- Databricks notebook source
SELECT 
  * 
FROM 
  silver.olist.pagamento_pedido 

-- COMMAND ----------

SELECT 
  * 
FROM
  silver.olist.pagamento_pedido t2  

-- COMMAND ----------

SELECT 
  * 
FROM
  silver.olist.item_pedido t3

-- COMMAND ----------

WITH TblFull AS (
SELECT 
  t3.idVendedor,
  t2.*
FROM 
  silver.olist.pedido t1 
LEFT JOIN 
  silver.olist.pagamento_pedido t2 
    ON t1.idPedido = t2.idPedido
LEFT JOIN 
  silver.olist.item_pedido t3 
    ON t1.idPedido = t3.idPedido
WHERE 
  dtPedido >= add_months('2018-01-01', -6)
AND 
  dtPedido < '2018-01-01'),  

TblGroup AS (
SELECT 
  IdVendedor, 
  descTipoPagamento, 
  count(DISTINCT IdPedido) AS QtyPedido, 
  sum(vlPagamento) AS VolumePagamentos 
FROM 
  TblFull 
GROUP BY 
  IdVendedor, 
  descTipoPagamento) 


SELECT 
  IdVendedor, 
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN QtyPedido ELSE 0 END) AS Qty_boleto,
  SUM(CASE WHEN descTipoPagamento = 'credit_card'  THEN QtyPedido ELSE 0 END) AS Qty_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN QtyPedido ELSE 0 END) AS Qty_voucher,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN QtyPedido ELSE 0 END) AS Qty_debit_card, 

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN VolumePagamentos ELSE 0 END) AS Vlm_boleto, 
  SUM(CASE WHEN descTipoPagamento = 'credit_card'  THEN VolumePagamentos ELSE 0 END) AS Vlm_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN VolumePagamentos ELSE 0 END) AS Vlm_voucher,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN VolumePagamentos ELSE 0 END) AS Vlm_debit_card, 

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN QtyPedido ELSE 0 END) / SUM(QtyPedido) AS perc_Qty_boleto,
  SUM(CASE WHEN descTipoPagamento = 'credit_card'  THEN QtyPedido ELSE 0 END) / SUM(QtyPedido) AS perc_Qty_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN QtyPedido ELSE 0 END) / SUM(QtyPedido) AS perc_Qty_voucher,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN QtyPedido ELSE 0 END) / SUM(QtyPedido)AS perc_Qty_debit_card,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN VolumePagamentos ELSE 0 END) / SUM(VolumePagamentos) AS perc_Vlm_boleto, 
  SUM(CASE WHEN descTipoPagamento = 'credit_card'  THEN VolumePagamentos ELSE 0 END) / SUM(VolumePagamentos)AS perc_Vlm_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN VolumePagamentos ELSE 0 END) / SUM(VolumePagamentos)AS perc_Vlm_voucher,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN VolumePagamentos ELSE 0 END) / SUM(VolumePagamentos) AS perc_Vlm_debit_card 

FROM 
  TblGroup
GROUP BY 
  IdVendedor; 
