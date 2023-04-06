-- Databricks notebook source
with tb_join as(
select distinct t1.idPedido
        , t1.idCliente
        , t2.idVendedor
        , t3.descUF
from silver.olist.pedido t1
left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
left join silver.olist.cliente t3 on t1.idCLiente = t3.idCliente
where dtPedido < '2018-01-01' and dtPedido >= add_months('2018-01-01', -6)
and idVendedor is not null
),
tb_group as(
select idVendedor
      , count(distinct descUF) as qtdUfsPedidos
      , count(distinct case when descUF = 'AC' then idPedido end) / count(distinct idPedido) as pctClienteAC
      , count(distinct case when descUF = 'AL' then idPedido end) / count(distinct idPedido) as pctClienteAL
      , count(distinct case when descUF = 'AM' then idPedido end) / count(distinct idPedido) as pctClienteAM
      , count(distinct case when descUF = 'AP' then idPedido end) / count(distinct idPedido) as pctClienteAP
      , count(distinct case when descUF = 'BA' then idPedido end) / count(distinct idPedido) as pctClienteBA
      , count(distinct case when descUF = 'CE' then idPedido end) / count(distinct idPedido) as pctClienteCE
      , count(distinct case when descUF = 'DF' then idPedido end) / count(distinct idPedido) as pctClienteDF
      , count(distinct case when descUF = 'ES' then idPedido end) / count(distinct idPedido) as pctClienteES
      , count(distinct case when descUF = 'GO' then idPedido end) / count(distinct idPedido) as pctClienteGO
      , count(distinct case when descUF = 'MA' then idPedido end) / count(distinct idPedido) as pctClienteMA
      , count(distinct case when descUF = 'MG' then idPedido end) / count(distinct idPedido) as pctClienteMG
      , count(distinct case when descUF = 'MS' then idPedido end) / count(distinct idPedido) as pctClienteMS
      , count(distinct case when descUF = 'MT' then idPedido end) / count(distinct idPedido) as pctClienteMT
      , count(distinct case when descUF = 'PA' then idPedido end) / count(distinct idPedido) as pctClientePA
      , count(distinct case when descUF = 'PB' then idPedido end) / count(distinct idPedido) as pctClientePB
      , count(distinct case when descUF = 'PE' then idPedido end) / count(distinct idPedido) as pctClientePE
      , count(distinct case when descUF = 'PI' then idPedido end) / count(distinct idPedido) as pctClientePI
      , count(distinct case when descUF = 'PR' then idPedido end) / count(distinct idPedido) as pctClientePR
      , count(distinct case when descUF = 'RJ' then idPedido end) / count(distinct idPedido) as pctClienteRJ
      , count(distinct case when descUF = 'RN' then idPedido end) / count(distinct idPedido) as pctClienteRN
      , count(distinct case when descUF = 'RO' then idPedido end) / count(distinct idPedido) as pctClienteRO
      , count(distinct case when descUF = 'RR' then idPedido end) / count(distinct idPedido) as pctClienteRR
      , count(distinct case when descUF = 'RS' then idPedido end) / count(distinct idPedido) as pctClienteRS
      , count(distinct case when descUF = 'SC' then idPedido end) / count(distinct idPedido) as pctClienteSC
      , count(distinct case when descUF = 'SE' then idPedido end) / count(distinct idPedido) as pctClienteSE
      , count(distinct case when descUF = 'SP' then idPedido end) / count(distinct idPedido) as pctClienteSP
      , count(distinct case when descUF = 'TO' then idPedido end) / count(distinct idPedido) as pctClienteTO
from tb_join
group by idVendedor
)

select '2018-01-01' as dtReference
      , *
from tb_group
