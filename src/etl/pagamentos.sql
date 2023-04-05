-- Databricks notebook source
select 
distinct
t1.idPedido,
t2.idVendedor

from silver.olist.pedido as t1

left join silver.olist.item_pedido as t2
on t1.idpedido = t2.idpedido

where t1.dtpedido < '2018-01-01' and t1.dtpedido >= add_months('2018-01-01', -6)

-- COMMAND ----------

with tb_pedidos as(
select 
distinct
t1.idPedido,
t2.idVendedor

from silver.olist.pedido as t1

left join silver.olist.item_pedido as t2
on t1.idpedido = t2.idpedido

where t1.dtpedido < '2018-01-01' and t1.dtpedido >= add_months('2018-01-01', -6)
and idVendedor is not null
),

tb_join as(
select 
t2.*,
t1.idVendedor
from tb_pedidos as t1
left join silver.olist.pagamento_pedido as t2
on t1.idpedido = t2.idpedido
),

tb_group as(
select 
idvendedor, 
desctipopagamento,
count(distinct idpedido) as qtd_pedido_meio_pagamento,
sum(vlpagamento) as vl_pedido_meio_pagamento 
from tb_join

group by idvendedor, desctipopagamento
),

tb_summary as(
select 
idvendedor,
sum(case when desctipopagamento =  'boleto' then qtd_pedido_meio_pagamento else 0 end) as qtd_boleto,
sum(case when desctipopagamento =  'credit_card' then qtd_pedido_meio_pagamento else 0 end) as qtd_credit_card,
sum(case when desctipopagamento =  'voucher' then qtd_pedido_meio_pagamento else 0 end) as qtd_voucher,
sum(case when desctipopagamento =  'debit_card' then qtd_pedido_meio_pagamento else 0 end) as qtd_debit_card,

sum(case when desctipopagamento =  'boleto' then vl_pedido_meio_pagamento else 0 end) as qtd_vl_boleto,
sum(case when desctipopagamento =  'credit_card' then vl_pedido_meio_pagamento else 0 end) as qtd_vl_credit_card,
sum(case when desctipopagamento =  'voucher' then vl_pedido_meio_pagamento else 0 end) as qtd_vl_voucher,
sum(case when desctipopagamento =  'debit_card' then vl_pedido_meio_pagamento else 0 end) as qtd_vl_debit_card,

sum(case when desctipopagamento =  'boleto' then qtd_pedido_meio_pagamento else 0 end) / sum(qtd_pedido_meio_pagamento ) as prop_qtd_boleto,
sum(case when desctipopagamento =  'credit_card' then qtd_pedido_meio_pagamento else 0 end) / sum(qtd_pedido_meio_pagamento ) as prop_qtd_credit_card,
sum(case when desctipopagamento =  'voucher' then qtd_pedido_meio_pagamento else 0 end) / sum(qtd_pedido_meio_pagamento ) as prop_qtd_voucher,
sum(case when desctipopagamento =  'debit_card' then qtd_pedido_meio_pagamento else 0 end) / sum(qtd_pedido_meio_pagamento ) as prop_qtd_debit_card,

sum(case when desctipopagamento =  'boleto' then vl_pedido_meio_pagamento else 0 end) / sum(vl_pedido_meio_pagamento) as prop_qtd_vl_boleto,
sum(case when desctipopagamento =  'credit_card' then vl_pedido_meio_pagamento else 0 end) / sum(vl_pedido_meio_pagamento) as prop_qtd_vl_credit_card,
sum(case when desctipopagamento =  'voucher' then vl_pedido_meio_pagamento else 0 end) / sum(vl_pedido_meio_pagamento) as prop_qtd_vl_voucher,
sum(case when desctipopagamento =  'debit_card' then vl_pedido_meio_pagamento else 0 end) / sum(vl_pedido_meio_pagamento) as prop_qtd_vl_debit_card
from tb_group
group by idvendedor
),
tb_cartao as(
select
idVendedor,
avg(nrParcelas) avgQtdParcelas,
percentile(nrParcelas, 0.5) as medianQtdParcelas,
max(nrParcelas) as max_parcelas,
min(nrParcelas) as min_parcelas

from tb_join

where descTipoPagamento = 'credit_card'

group by idVendedor
)
select
'2018-01-01' as dtReference,
t1.*,
t2.*
from tb_summary as t1

left join tb_cartao as t2
on t1.idvendedor = t2.idvendedor


