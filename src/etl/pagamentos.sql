-- Databricks notebook source
SELECT DISTINCT t1.idPedido
	,t2.idVendedor
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
WHERE t1.dtPedido <= '2018-01-01'
	AND t1.dtPedido >= add_months('2018-01-01', - 6)

-- COMMAND ----------

-- DBTITLE 1, 
WITH tb_pedidos
AS (
	SELECT DISTINCT t1.idPedido
		,t2.idVendedor
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
	WHERE t1.dtPedido <= '2018-01-01'
		AND t1.dtPedido >= add_months('2018-01-01', - 6)
		AND idVendedor IS NOT NULL
	)
	,tb_join
AS (
	SELECT t1.idVendedor
		,t2.*
	FROM tb_pedidos AS t1
	LEFT JOIN silver.olist.pagamento_pedido AS t2 ON t1.idPedido = t2.idPedido
	)
	,tb_group
AS (
	SELECT idVendedor
		,descTipoPagamento
		,count(DISTINCT idPedido) AS qtdePedidoMeioPagamento
		,sum(vlPagamento) AS vlPedidoMeioPagamento
	FROM tb_join
	GROUP BY idVendedor
		,descTipoPagamento
	ORDER BY idVendedor
		,descTipoPagamento
	)
	,tb_summary
AS (
	SELECT idVendedor
		,sum(CASE 
				WHEN descTipoPagamento = 'boleto'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) AS qtde_boleto_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'credit_card'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) AS qtde_credit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'voucher'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) AS qtde_voucher_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'debit_card'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) AS qtde_debit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'boleto'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) AS vl_boleto_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'credit_card'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) AS vl_credit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'voucher'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) AS vl_voucher_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'debit_card'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) AS vl_debit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'boleto'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) / sum(qtdePedidoMeioPagamento) AS pct_qtde_boleto_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'credit_card'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) / sum(qtdePedidoMeioPagamento) AS pct_qtde_credit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'voucher'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) / sum(qtdePedidoMeioPagamento) AS pct_qtde_voucher_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'debit_card'
					THEN qtdePedidoMeioPagamento
				ELSE 0
				END) / sum(qtdePedidoMeioPagamento) AS pct_qtde_debit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'boleto'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) / sum(vlPedidoMeioPagamento) AS pct_vl_boleto_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'credit_card'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) / sum(vlPedidoMeioPagamento) AS pct_vl_credit_card_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'voucher'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) / sum(vlPedidoMeioPagamento) AS pct_vl_voucher_pedido
		,sum(CASE 
				WHEN descTipoPagamento = 'debit_card'
					THEN vlPedidoMeioPagamento
				ELSE 0
				END) / sum(vlPedidoMeioPagamento) AS pct_vl_debit_card_pedido
	FROM tb_group
	GROUP BY idVendedor
	)
	,tb_cartao
AS (
	SELECT idVendedor
		,AVG(nrParcelas) AS avgQtdeParcelas
		,PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas
		,MAX(nrParcelas) AS maxQtdeParcelas
		,MIN(nrParcelas) AS minQtdeParcelas
	FROM tb_join
	WHERE descTipoPagamento = 'credit_card'
	GROUP BY idVendedor
	)
SELECT '2018-01-01' AS dtReference
	,t1.*
	,t2.avgQtdeParcelas
	,t2.medianQtdeParcelas
	,t2.maxQtdeParcelas
	,t2.minQtdeParcelas
FROM tb_summary AS t1
LEFT JOIN tb_cartao AS t2 ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------


