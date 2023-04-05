-- Databricks notebook source
select Date(dtPedido) as stPedido,
count(*) as qtdPedido
from silver.olist.pedido
group by 1
order by 1


-- COMMAND ----------

--apenas pagamentos entre os ultimos 6 meses
select t2.*
from silver.olist.pedido

select *
from silver.olist.pedido
where dtPedido < '2018-01-01'
and dtPedido >= add_months('2018-01-01',-6) --não usou o between pq não é performático

-- COMMAND ----------

--dados do vendedor dos últimos 6 meses

WITH tb_pedidos AS (
--tabela responsável por trazer pedido nos últimos 6 meses e o vendedor 
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
--tabela para trazer todas as informações sobre pagamento: tipo de pagamento, valor, parcelas de cartão
  SELECT 
        t1.idVendedor,
        t2.*         

  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

),

tb_group AS (
--tabela para trazer a qtd de pedidos e somatório de valor de cada meio de pagamento para cada vendedor
  SELECT idVendedor,
         descTipoPagamento,
         count(distinct idPedido) as qtdePedidoMeioPagamento,
         sum(vlPagamento) as vlPedidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento

),

tb_summary AS (

  SELECT 
    idVendedor,
    --criar uma coluna para cada tipo de pagamento com as qtd de pagamento
    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

    --criar uma coluna para cada tipo de pagamento com o valor pagamento
    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,
    
    --porcentagem por tipo de pagamento
    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido

  FROM tb_group

  GROUP BY idVendedor

),

tb_cartao as (
--tabela para trazer as estatísticas de parcelas de cartão
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
