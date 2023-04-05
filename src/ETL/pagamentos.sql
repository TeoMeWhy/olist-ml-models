-- Databricks notebook source
WITH tabela_consolida AS 

(
      SELECT

             T2.*
            ,T3.idVendedor

      FROM silver.olist.pedido AS T1

      LEFT JOIN silver.olist.pagamento_pedido  AS T2 ON T1.idPedido = T2.idPedido
      LEFT JOIN silver.olist.item_pedido       AS T3 ON T1.idPedido = T3.idPedido

      WHERE T1.dtPedido < '2018-01-01' AND T1.dtPedido >= add_months('2018-01-01', -6) 
      AND   T3.idVendedor IS NOT NULL
 ),

 
 
 
tabela_agrupada AS

(  

SELECT 
         idVendedor
        ,descTipoPagamento
        ,COUNT(DISTINCT idPedido) AS qtdPedidoMeioPagamento
        ,SUM(vlPagamento)         AS vlPagamentoMeioPagamento
        ,SUM(nrParcelas) AS Parcelas
        
FROM tabela_consolida  
GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento


)

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
      
     ,SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdPedidoMeioPagamento ELSE 0 END) / SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN Parcelas ELSE 0 END) AS qntd_media_parcelas_cartao_credito
     
FROM tabela_agrupada
GROUP BY idVendedor


-- COMMAND ----------



-- COMMAND ----------

SELECT 
         DATE(dtPedido) as data_pedido
        ,COUNT(*)       as pedidos
        
FROM silver.olist.pedido 

GROUP BY 1
ORDER BY 1

-- COMMAND ----------


