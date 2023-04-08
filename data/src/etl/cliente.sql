WITH tb_join AS (
    SELECT DISTINCT
            t1.idPedido,
            t1.idCliente,
            t2.idVendedor,
            t3.descUF

    FROM pedido AS t1

    LEFT JOIN item_pedido as t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN cliente as t3
    ON t1.idCliente = t3.idCliente

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01', '-6 MONTH')
    AND t2.idVendedor IS NOT NULL
),

tb_group AS (
    SELECT
        idVendedor,
        
        count(distinct descUF) as qtdUFsPedidos,

        CAST(count(distinct case when descUF = 'AC' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoAC,
        CAST(count(distinct case when descUF = 'AL' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoAL,
        CAST(count(distinct case when descUF = 'AM' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoAM,
        CAST(count(distinct case when descUF = 'AP' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoAP,
        CAST(count(distinct case when descUF = 'BA' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoBA,
        CAST(count(distinct case when descUF = 'CE' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoCE,
        CAST(count(distinct case when descUF = 'DF' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoDF,
        CAST(count(distinct case when descUF = 'ES' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoES,
        CAST(count(distinct case when descUF = 'GO' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoGO,
        CAST(count(distinct case when descUF = 'MA' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoMA,
        CAST(count(distinct case when descUF = 'MG' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoMG,
        CAST(count(distinct case when descUF = 'MS' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoMS,
        CAST(count(distinct case when descUF = 'MT' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoMT,
        CAST(count(distinct case when descUF = 'PA' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoPA,
        CAST(count(distinct case when descUF = 'PB' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoPB,
        CAST(count(distinct case when descUF = 'PE' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoPE,
        CAST(count(distinct case when descUF = 'PI' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoPI,
        CAST(count(distinct case when descUF = 'PR' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoPR,
        CAST(count(distinct case when descUF = 'RJ' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoRJ,
        CAST(count(distinct case when descUF = 'RN' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoRN,
        CAST(count(distinct case when descUF = 'RO' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoRO,
        CAST(count(distinct case when descUF = 'RR' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoRR,
        CAST(count(distinct case when descUF = 'RS' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoRS,
        CAST(count(distinct case when descUF = 'SC' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoSC,
        CAST(count(distinct case when descUF = 'SE' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoSE,
        CAST(count(distinct case when descUF = 'SP' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoSP,
        CAST(count(distinct case when descUF = 'TO' then idPedido end) AS REAL) / count(distinct idPedido) as pctPedidoTO

    FROM tb_join

    GROUP BY idVendedor
)

SELECT 
    '2018-01-01' AS dtReference,
    *

FROM tb_group