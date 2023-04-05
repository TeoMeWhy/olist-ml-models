-- Databricks notebook source
select *
from silver.olist.pedido_pagamento

-- COMMAND ----------

with tb_join as(
select 
t2.*,
t3.idvendedor


from silver.olist.pedido as t1

left join silver.olist.pagamento_pedido as t2
on t1.idpedido = t2.idpedido

left join silver.olist.item_pedido as t3
on t1.idpedido = t3.idpedido

where dtpedido < '2018-01-01' and dtpedido >= add_months('2018-01-01', -6)
and t3.idvendedor is not null
),
tb_group as(
select 
idvendedor, 
desctipopagamento,
count(distinct idpedido) as qtd_pedido_meio_pagamento,
sum(vlpagamento) as vl_pedido_meio_pagamento 
from tb_join

group by idvendedor, desctipopagamento
)
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
