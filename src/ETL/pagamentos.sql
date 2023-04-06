-- Databricks notebook source
WITH tb_pedidos AS (

  SELECT 
      DISTINCT 
      t1.idPedido,
      t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

),

tb_join AS (

  SELECT 
        t1.idVendedor,
        t2.*         

  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

),
 
 
tabela_agrupada AS

(  

SELECT 
         idVendedor
        ,descTipoPagamento
        ,COUNT(DISTINCT idPedido) AS qtdPedidoMeioPagamento
        ,SUM(vlPagamento)         AS vlPagamentoMeioPagamento
        ,SUM(nrParcelas) AS Parcelas
        
FROM tb_join
GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento


),

tb_summary AS (

SELECT  
      idVendedor
     ,SUM(CASE WHEN descTipoPagamento = 'boleto'      THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_pedido_Boleto  
     ,SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_pedido_CreditCard
     ,SUM(CASE WHEN descTipoPagamento = 'voucher'     THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_pedido_Voucher
     ,SUM(CASE WHEN descTipoPagamento = 'debit_card'  THEN qtdPedidoMeioPagamento ELSE 0 END) AS qtd_pedido_DebitCard
     
     ,SUM(CASE WHEN descTipoPagamento = 'boleto'      THEN vlPagamentoMeioPagamento ELSE 0 END) AS valor_pedido_Boleto  
     ,SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPagamentoMeioPagamento ELSE 0 END) AS valor_pedido_CreditCard
     ,SUM(CASE WHEN descTipoPagamento = 'voucher'     THEN vlPagamentoMeioPagamento ELSE 0 END) AS valor_pedido_Voucher
     ,SUM(CASE WHEN descTipoPagamento = 'debit_card'  THEN vlPagamentoMeioPagamento ELSE 0 END) AS valor_pedido_DebitCard
     
     ,SUM(CASE WHEN descTipoPagamento = 'boleto'      THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_pedido_Boleto  
     ,SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_pedido_CreditCard
     ,SUM(CASE WHEN descTipoPagamento = 'voucher'     THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_pedido_Voucher
     ,SUM(CASE WHEN descTipoPagamento = 'debit_card'  THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(qtdPedidoMeioPagamento) AS pct_qtd_pedido_DebitCard
     
     ,SUM(CASE WHEN descTipoPagamento = 'boleto'      THEN vlPagamentoMeioPagamento ELSE 0 END) / SUM(vlPagamentoMeioPagamento) AS pct_valor_pedido_Boleto  
     ,SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPagamentoMeioPagamento ELSE 0 END) / SUM(vlPagamentoMeioPagamento) AS pct_valor_pedido_CreditCard
     ,SUM(CASE WHEN descTipoPagamento = 'voucher'     THEN vlPagamentoMeioPagamento ELSE 0 END) / SUM(vlPagamentoMeioPagamento) AS pct_valor_pedido_Voucher
     ,SUM(CASE WHEN descTipoPagamento = 'debit_card'  THEN vlPagamentoMeioPagamento ELSE 0 END) / SUM(vlPagamentoMeioPagamento) AS pct_valor_pedido_DebitCard
      
     
FROM tabela_agrupada
GROUP BY idVendedor

),

tb_cartao as (

  SELECT idVendedor,
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

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor

