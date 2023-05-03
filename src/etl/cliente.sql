WITH tb_join AS (

    SELECT DISTINCT t1.idPedido,t1.idCliente,t2.idVendedor,t3.descUF FROM pedido AS t1
    LEFT JOIN item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN cliente as t3
    ON t1.idCliente = t3.idCliente

    WHERE dtPedido <= '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01', '-6 months')
    AND idVendedor IS NOT NULL

),

tb_group AS (

    SELECT idVendedor,
    COUNT(DISTINCT CASE WHEN descUF = 'AC' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoAC,
    COUNT(DISTINCT CASE WHEN descUF = 'AL' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoAL,
    COUNT(DISTINCT CASE WHEN descUF = 'AM' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoAM,
    COUNT(DISTINCT CASE WHEN descUF = 'AP' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoAP,
    COUNT(DISTINCT CASE WHEN descUF = 'BA' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoBA,
    COUNT(DISTINCT CASE WHEN descUF = 'CE' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoCE,
    COUNT(DISTINCT CASE WHEN descUF = 'DF' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoDF,
    COUNT(DISTINCT CASE WHEN descUF = 'ES' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoES,
    COUNT(DISTINCT CASE WHEN descUF = 'GO' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoGO,
    COUNT(DISTINCT CASE WHEN descUF = 'MA' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoMA,
    COUNT(DISTINCT CASE WHEN descUF = 'MG' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoMG,
    COUNT(DISTINCT CASE WHEN descUF = 'MS' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoMS,
    COUNT(DISTINCT CASE WHEN descUF = 'MT' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoMT,
    COUNT(DISTINCT CASE WHEN descUF = 'PA' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoPA,
    COUNT(DISTINCT CASE WHEN descUF = 'PB' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoPB,
    COUNT(DISTINCT CASE WHEN descUF = 'PE' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoPE,
    COUNT(DISTINCT CASE WHEN descUF = 'PI' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoPI,
    COUNT(DISTINCT CASE WHEN descUF = 'PR' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoPR,
    COUNT(DISTINCT CASE WHEN descUF = 'RJ' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoRJ,
    COUNT(DISTINCT CASE WHEN descUF = 'RN' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoRN,
    COUNT(DISTINCT CASE WHEN descUF = 'RO' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoRO,
    COUNT(DISTINCT CASE WHEN descUF = 'RR' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoRR,
    COUNT(DISTINCT CASE WHEN descUF = 'RS' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoRS,
    COUNT(DISTINCT CASE WHEN descUF = 'SC' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoSC,
    COUNT(DISTINCT CASE WHEN descUF = 'SE' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoSE,
    COUNT(DISTINCT CASE WHEN descUF = 'SP' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoSP,
    COUNT(DISTINCT CASE WHEN descUF = 'TO' then idPedido END)*1.0/COUNT(DISTINCT idPedido) as pctPedidoTO
    FROM tb_join
    GROUP BY idVendedor
)

SELECT '2018-01-01' as dtReference,*
FROM tb_group
