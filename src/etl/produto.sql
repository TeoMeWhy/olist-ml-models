-- Databricks notebook source
with tb_join as(
select distinct 
         t2.idVendedor
        , t3.*
from silver.olist.pedido t1
left join silver.olist.item_pedido t2 on t1.idPedido = t2.idPedido
left join silver.olist.produto t3 on t2.idProduto = t3.idProduto
where t1.dtPedido > '2018-01-01' and t1.dtPedido >= add_months('2018-01-01', -6)
and t2.idVendedor is not null
),
tb_summary as(
select idVendedor
      , avg(coalesce(nrFotos, 0)) as avgFotos
      , avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as avgVolumeProduto
      , percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) as medianVolumeProduto
      , min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as minVolumeProduto
      , max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as maxVolumeProduto
      , count(distinct case when descCategoria = 'cama_mesa_banho' then idProduto end) / count(distinct idProduto) as pctCaegoriacama_mesa_banho
      , count(distinct case when descCategoria = 'beleza_saude' then idProduto end) / count(distinct idProduto) as pctCaegoriabeleza_saude
      , count(distinct case when descCategoria = 'esporte_lazer' then idProduto end) / count(distinct idProduto) as pctCaegoriaesporte_lazer
      , count(distinct case when descCategoria = 'moveis_decoracao' then idProduto end) / count(distinct idProduto) as pctCaegoriamoveis_decoracao
      , count(distinct case when descCategoria = 'informatica_acessorios' then idProduto end) / count(distinct idProduto) as pctCaegoriainformatica_acessorios
      , count(distinct case when descCategoria = 'utilidades_domesticas' then idProduto end) / count(distinct idProduto) as pctCaegoriautilidades_domesticas
      , count(distinct case when descCategoria = 'relogios_presentes' then idProduto end) / count(distinct idProduto) as pctCaegoriarelogios_presentes
      , count(distinct case when descCategoria = 'telefonia' then idProduto end) / count(distinct idProduto) as pctCaegoriatelefonia
      , count(distinct case when descCategoria = 'ferramentas_jardim' then idProduto end) / count(distinct idProduto) as pctCaegoriaferramentas_jardim
      , count(distinct case when descCategoria = 'automotivo' then idProduto end) / count(distinct idProduto) as pctCaegoriaautomotivo
      , count(distinct case when descCategoria = 'brinquedos' then idProduto end) / count(distinct idProduto) as pctCaegoriabrinquedos
      , count(distinct case when descCategoria = 'cool_stuff' then idProduto end) / count(distinct idProduto) as pctCaegoriacool_stuff
      , count(distinct case when descCategoria = 'perfumaria' then idProduto end) / count(distinct idProduto) as pctCaegoriaperfumaria
      , count(distinct case when descCategoria = 'bebes' then idProduto end) / count(distinct idProduto) as pctCaegoriabebes
      , count(distinct case when descCategoria = 'eletronicos' then idProduto end) / count(distinct idProduto) as pctCaegoriaeletronicos
from tb_join
group by idVendedor
)
select '2018-01-01' as dtReference
        , *
from tb_summary

-- COMMAND ----------

select descCategoria
from silver.olist.item_pedido t2 
left join silver.olist.produto t3 on t2.idProduto = t3.idProduto
and t2.idVendedor is not null
group by 1
order by count(idPedido) desc
limit 15
