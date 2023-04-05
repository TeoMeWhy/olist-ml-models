-- Databricks notebook source
-- MAGIC %md ## Issues
-- MAGIC 
-- MAGIC - % formas de pagamentos (para cada vendedor)
-- MAGIC - Quantidade média de parcelas (quando cartão) (para cada vendedor)

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
      AND t1.dtPedido >= add_months ('2018-01-01', -6)
      AND t3.idVendedor IS NOT NULL),
      
tb_group AS (

SELECT idVendedor, 
       descTipoPagamento,
       count(distinct idPedido) as qtdePedidoMeioPagamento, -- Contagem de pedidos por meio de pagamento
       sum(vlPagamento) AS vlPedidoMeioPagamento -- soma de todas as vendas por tipo de pagamento
       
FROM tb_join

GROUP BY 1, 2
ORDER BY 1, 2

)

SELECT idVendedor,
       -- soma dos pedidos por tipo de pagamento
       sum(CASE WHEN descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento 
                                                  else 0 end) as qtde_pedido_boleto,
       sum(CASE WHEN descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento 
                                                       else 0 end) as qtde_pedido_credit_card,
       sum(CASE WHEN descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento 
                                                   else 0 end) as qtde_pedido_voucher,
       sum(CASE WHEN descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento 
                                                      else 0 end) as qtde_pedido_debit_card,
       -- soma dos valores por meio de pagamento                                            
       sum(CASE WHEN descTipoPagamento = 'boleto' then vlPedidoMeioPagamento 
                                                   else 0 end) as valor_pedido_boleto,
       sum(CASE WHEN descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento 
                                                       else 0 end) as valor_pedido_credit_card,
       sum(CASE WHEN descTipoPagamento = 'voucher' then vlPedidoMeioPagamento 
                                                   else 0 end) as valor_pedido_voucher,
       sum(CASE WHEN descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento 
                                                      else 0 end) as valor_pedido_debit_card,
       -- proporcao dos pedidos por tipo de pagamento                                               
       sum(CASE WHEN descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento 
                                                  else 0 end) / sum(qtdePedidoMeioPagamento) as prop_pedido_boleto,
       sum(CASE WHEN descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento 
                                                       else 0 end) / sum(qtdePedidoMeioPagamento) as prop_pedido_credit_card,
       sum(CASE WHEN descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento 
                                                   else 0 end) / sum(qtdePedidoMeioPagamento) as prop_pedido_voucher,
       sum(CASE WHEN descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento 
                                                      else 0 end) / sum(qtdePedidoMeioPagamento) as qtde_pedido_debit_card,
       -- proporcao dos valores por meio de pagamento                                            
       sum(CASE WHEN descTipoPagamento = 'boleto' then vlPedidoMeioPagamento 
                                                   else 0 end) / sum(vlPedidoMeioPagamento) as prop_pedido_boleto,
       sum(CASE WHEN descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento 
                                                       else 0 end) / sum(vlPedidoMeioPagamento) as prop_pedido_credit_card,
       sum(CASE WHEN descTipoPagamento = 'voucher' then vlPedidoMeioPagamento 
                                                   else 0 end) / sum(vlPedidoMeioPagamento) as prop_pedido_voucher,
       sum(CASE WHEN descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento 
                                                      else 0 end) / sum(vlPedidoMeioPagamento) as prop_pedido_debit_card                                             
                                                      
       
FROM tb_group
GROUP BY 1

-- COMMAND ----------


