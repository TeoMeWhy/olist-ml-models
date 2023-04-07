WITH tb_pedidos AS (
    SELECT
        DISTINCT
            t1.idPedido,
            t2.idVendedor

    FROM pedido AS t1

    LEFT JOIN item_pedido as t2
    ON t1.idPedido = t2.idPedido

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01', '-6 MONTH')
    AND t2.idVendedor IS NOT NULL
),

tb_join AS (
    SELECT
        t1.idVendedor,
        t2.*

    FROM tb_pedidos AS t1

    LEFT JOIN pagamento_pedido as t2
    ON t1.idPedido = t2.idPedido

),

tb_group AS (
    SELECT
        idVendedor,
        descTipoPagamento,
        count(distinct idPedido) as qtdPedidoMeioPagamento,
        sum(vlPagamento) as vlPedidoMeioPagamento

    FROM tb_join

    GROUP BY 1,2
    ORDER BY 1,2 
),

tb_sumary AS (
    SELECT
        idVendedor,
        sum(case when descTipoPagamento = 'credit_card' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as 'qtd_credit_card',
        sum(case when descTipoPagamento = 'boleto' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as 'qtd_boleto',
        sum(case when descTipoPagamento = 'debit_card' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as 'qtd_debit_card',
        sum(case when descTipoPagamento = 'voucher' then qtdPedidoMeioPagamento else 0 end) / sum(qtdPedidoMeioPagamento) as 'qtd_voucher',

        sum(case when descTipoPagamento = 'credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as 'vl_credit_card',
        sum(case when descTipoPagamento = 'boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as 'vl_boleto',
        sum(case when descTipoPagamento = 'debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as 'vl_debit_card',
        sum(case when descTipoPagamento = 'voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as 'vl_voucher'

    FROM tb_group
    GROUP BY 1
),

tb_cartao AS (
    SELECT 
    idVendedor,
    AVG(nrParcelas) as avgQtdParcelas,
    (SELECT 
         AVG(nrParcelas) 
     FROM tb_join 
     WHERE descTipoPagamento = 'credit_card' AND t1.idVendedor = tb_join.idVendedor
       AND (SELECT COUNT(*) FROM tb_join WHERE descTipoPagamento = 'credit_card' AND idVendedor = t1.idVendedor) / 2 = 
         (SELECT COUNT(*) FROM tb_join WHERE descTipoPagamento = 'credit_card' AND idVendedor = t1.idVendedor 
              AND nrParcelas <= t1.nrParcelas)
    ) as medianQtdParcelas,
    MAX(nrParcelas) as maxQtdParcelas,
    MIN(nrParcelas) as minQtdParcelas
FROM 
    tb_join t1
WHERE 
    descTipoPagamento = 'credit_card'
GROUP BY 
    idVendedor
)

SELECT
    DATE('2018-01-01') AS dtReference,
    t1.*,
    t2.avgQtdParcelas,
    t2.medianQtdParcelas,
    t2.maxQtdParcelas,
    t2.minQtdParcelas

    FROM tb_sumary as t1
    LEFT JOIN tb_cartao as t2
    ON t1.idVendedor = t2.idVendedor
;